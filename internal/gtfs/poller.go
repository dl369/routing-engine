// Package gtfs provides both a high-performance static GTFS parser (parser.go)
// and a concurrent real-time GTFS-RT poller (poller.go) for the Route transit
// engine.
//
// Poller design goals:
//   - Fan out all endpoint fetches in parallel on every tick so network latency
//     across five endpoints is bounded by the slowest single fetch, not their
//     sum.
//   - Re-use a single http.Client (and its underlying TCP connection pool) across
//     ticks so we pay no TLS handshake overhead after the first poll.
//   - Hard-cap every response body at 32 MiB via io.LimitReader to prevent a
//     misbehaving upstream from exhausting heap.
//   - Validate each payload by unmarshalling the protobuf, then store the raw
//     proto bytes in Redis — downstream readers can unmarshal with zero extra
//     serialisation cost.
package gtfs

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// ─── Constants ───────────────────────────────────────────────────────────────

const (
	// maxBodyBytes is the hard ceiling applied to every HTTP response body via
	// io.LimitReader. 32 MiB is generous for any individual GTFS-RT feed but
	// tight enough to prevent OOM from a runaway upstream or a gzip bomb whose
	// compressed form slips under the wire while decompressing to gigabytes.
	maxBodyBytes = 32 << 20 // 32 MiB

	// redisKeyTTL is the Time-To-Live attached to each GTFS-RT key in Redis.
	// Two missed consecutive polls (2 × 15 s = 30 s) before eviction gives
	// downstream consumers a brief grace window while still guaranteeing that
	// indefinitely stale data cannot accumulate if the poller goes offline.
	redisKeyTTL = 30 * time.Second

	// pollDeadlineRatio is the fraction of the poll interval used as the
	// per-round context deadline. 0.8 of 15 s = 12 s, leaving a 3 s buffer
	// before the next tick fires so that goroutine cleanup completes first.
	pollDeadlineRatio = 0.8
)

// ─── Endpoint table ──────────────────────────────────────────────────────────

// rtEndpoint pairs a single GTFS-RT source URL with the Redis key that stores
// its latest FeedMessage payload.
type rtEndpoint struct {
	URL      string
	RedisKey string
}

// rtEndpoints is the canonical, ordered list of TfNSW real-time feeds.
//
// The slice is a package-level value: it is allocated once at program start and
// treated as read-only, so zero per-tick allocation is required to range over it.
var rtEndpoints = []rtEndpoint{
	// v1 bus network — high-frequency urban/suburban buses.
	{
		URL:      "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses",
		RedisKey: "gtfsrt:buses",
	},
	// v1 CBD & South East Light Rail.
	{
		URL:      "https://api.transport.nsw.gov.au/v1/gtfs/realtime/lightrail/cbdandsoutheast",
		RedisKey: "gtfsrt:cbdlightrail",
	},
	// v2 Sydney Trains (intercity and suburban heavy rail).
	{
		URL:      "https://api.transport.nsw.gov.au/v2/gtfs/realtime/sydneytrains",
		RedisKey: "gtfsrt:sydneytrains",
	},
	// v2 Sydney Metro.
	{
		URL:      "https://api.transport.nsw.gov.au/v2/gtfs/realtime/metro",
		RedisKey: "gtfsrt:metro",
	},
	// v2 Inner West Light Rail.
	{
		URL:      "https://api.transport.nsw.gov.au/v2/gtfs/realtime/lightrail/innerwest",
		RedisKey: "gtfsrt:innerwestlightrail",
	},
}

// RTFeedKeys returns the Redis keys for every polled GTFS-RT endpoint.
func RTFeedKeys() []string {
	keys := make([]string, len(rtEndpoints))
	for i, ep := range rtEndpoints {
		keys[i] = ep.RedisKey
	}
	return keys
}

// ─── HTTP client ─────────────────────────────────────────────────────────────

// rtHTTPClient is the shared client used by every fetch goroutine.
//
// A single *http.Client means the underlying Transport — and its connection pool
// — is shared across all goroutines and across all ticks. TCP connections to
// api.transport.nsw.gov.au are kept alive and reused so the hot path after the
// first poll incurs no TLS handshake or TCP setup cost.
var rtHTTPClient = &http.Client{
	// 10 s per-request wall clock. This must be comfortably shorter than the
	// poll interval (15 s) so that a slow endpoint does not cause the WaitGroup
	// to outlive the tick.
	Timeout: 10 * time.Second,

	Transport: &http.Transport{
		// One idle connection per endpoint so every goroutine in the fan-out
		// can immediately grab a keep-alive connection without blocking.
		MaxIdleConnsPerHost: len(rtEndpoints),

		// DisableCompression = false (default) lets the transport automatically
		// negotiate gzip with the server. We handle decompression ourselves in
		// downloadFeed when the response carries Content-Encoding: gzip, because
		// we need to position the io.LimitReader correctly.
		DisableCompression: false,
	},
}

// ─── Redis initialiser ───────────────────────────────────────────────────────

// InitRedis creates a Redis client from the REDIS_ADDR (default "localhost:6379")
// and REDIS_PASSWORD environment variables, pings the server to confirm
// connectivity, and returns the ready-to-use client.
//
// The returned *redis.Client is safe for concurrent use by multiple goroutines.
// Callers should defer rdb.Close() on the client they receive.
func InitRedis() (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		// Sensible default for local development; override via env in production.
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,

		// Pool size: one connection per concurrent endpoint goroutine plus two
		// spares so SET commands never queue behind each other.
		PoolSize:     len(rtEndpoints) + 2,
		MinIdleConns: 2,
	})

	// Fail fast: verify the connection before handing the client to the caller.
	// A 5 s deadline here is generous; Redis on localhost typically responds
	// in < 1 ms.
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("gtfs: redis ping %q: %w", addr, err)
	}

	log.Printf("gtfs-rt: redis connected at %s", addr)
	return rdb, nil
}

// ─── Worker entry point ──────────────────────────────────────────────────────

// StartRTWorker launches the GTFS-RT polling loop. It polls all endpoints
// immediately on start-up and then on every interval tick.
//
// The function blocks until ctx is cancelled; the caller should run it in a
// dedicated goroutine:
//
//	go gtfs.StartRTWorker(ctx, rdb, 15*time.Second)
//
// On cancellation the current in-flight round is allowed to finish (bounded by
// the per-round deadline) before the function returns.
func StartRTWorker(ctx context.Context, rdb *redis.Client, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("gtfs-rt: worker started — interval=%v feeds=%d", interval, len(rtEndpoints))

	// Poll once immediately so the router has live data from second 0 rather
	// than waiting up to one full interval for the first tick.
	pollAll(ctx, rdb, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("gtfs-rt: worker stopping: %v", ctx.Err())
			return
		case <-ticker.C:
			pollAll(ctx, rdb, interval)
		}
	}
}

// ─── Fan-out poll ────────────────────────────────────────────────────────────

// pollAll launches one fetchAndPublish goroutine per endpoint concurrently,
// waits for all to complete, then logs a summary line.
//
// A child context with a deadline derived from interval × pollDeadlineRatio
// (12 s for a 15 s interval) is passed to every goroutine. This hard-caps the
// total wall-clock budget for one poll round and ensures no goroutine can bleed
// network activity into the subsequent tick.
func pollAll(ctx context.Context, rdb *redis.Client, interval time.Duration) {
	deadline := time.Duration(float64(interval) * pollDeadlineRatio)
	pollCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex // guards the counters below
		successCount int
		errCount     int
	)

	for _, ep := range rtEndpoints {
		ep := ep // pin loop variable for the goroutine closure
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := fetchAndPublish(pollCtx, rdb, ep); err != nil {
				log.Printf("gtfs-rt: poll error [%s]: %v", ep.RedisKey, err)
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()
		}()
	}

	wg.Wait()
	log.Printf("gtfs-rt: poll complete — ok=%d err=%d", successCount, errCount)
}

// ─── Per-endpoint fetch + publish ────────────────────────────────────────────

// fetchAndPublish downloads one GTFS-RT feed, validates the payload by
// unmarshalling the protobuf, and atomically writes the raw proto bytes to
// Redis under the endpoint's designated key.
//
// Storing raw proto bytes (rather than re-serialised JSON) means downstream
// consumers pay zero extra serialisation cost: they call proto.Unmarshal once
// against the bytes already sitting in Redis.
func fetchAndPublish(ctx context.Context, rdb *redis.Client, ep rtEndpoint) error {
	raw, err := downloadFeed(ctx, ep.URL)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}

	// Unmarshal to validate structural integrity. If a partial or truncated
	// payload reaches this point we surface the error here rather than silently
	// overwriting a healthy Redis key with corrupt data.
	var msg gtfsrt.FeedMessage
	if err := proto.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("proto unmarshal: %w", err)
	}

	// Write to Redis with a short TTL. SET is atomic, so downstream readers
	// always see either the old complete payload or the new complete payload —
	// never a partially written one.
	if err := rdb.Set(ctx, ep.RedisKey, raw, redisKeyTTL).Err(); err != nil {
		return fmt.Errorf("redis SET %s: %w", ep.RedisKey, err)
	}

	log.Printf("gtfs-rt: updated %s — entities=%d bytes=%d",
		ep.RedisKey, len(msg.GetEntity()), len(raw))

	return nil
}

// ─── HTTP download ────────────────────────────────────────────────────────────

// downloadFeed issues an authenticated HTTP GET to url and returns the raw
// (decompressed) protobuf bytes.
//
// Three defensive measures are applied in order:
//  1. The request carries the TfNSW API key from the TFNSW_API_KEY env var.
//  2. The response body is decompressed if the server signals gzip encoding.
//  3. io.LimitReader caps the decompressed read at maxBodyBytes (32 MiB) so a
//     gzip bomb or unexpectedly large payload cannot exhaust the heap.
//
// Non-2xx HTTP responses are returned as errors so upstream outages are surfaced
// in the log rather than silently leaving the Redis key stale.
func downloadFeed(ctx context.Context, url string) ([]byte, error) {
	// Build the request, wiring it to ctx so the http.Client respects both the
	// per-request Timeout on rtHTTPClient and the per-round pollCtx deadline.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	// TfNSW requires every request to carry an API key in the Authorization
	// header using the "apikey" scheme.
	apiKey := os.Getenv("TFNSW_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("TFNSW_API_KEY environment variable is not set")
	}
	req.Header.Set("Authorization", "apikey "+apiKey)

	// Request gzip-compressed responses to reduce network transfer bytes.
	// We set this header explicitly so that we retain control over when and how
	// decompression occurs (specifically, so we can interpose io.LimitReader
	// after decompression rather than before it).
	//
	// NOTE: Because we are explicitly setting Accept-Encoding, Go's Transport
	// will NOT auto-decompress the response body. We decompress manually below.
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := rtHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}

	// Select the right reader: decompress only if the server honoured our
	// Accept-Encoding request and signals gzip in Content-Encoding.
	var bodyReader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		bodyReader = gz
	}

	// Enforce the 32 MiB ceiling on the (already decompressed) byte stream.
	// We read maxBodyBytes+1 bytes: if we receive that many the content was
	// silently truncated, and we return an error rather than passing a corrupt
	// proto to the unmarshaller.
	limited := io.LimitReader(bodyReader, maxBodyBytes+1)

	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if int64(len(data)) > maxBodyBytes {
		return nil, fmt.Errorf("response body exceeds %d-byte hard limit", maxBodyBytes)
	}

	return data, nil
}

// ─── Sample logger ───────────────────────────────────────────────────────────

// logFeedSample prints up to the first 3 entities from a FeedMessage so the
// operator can do a quick visual spot-check of live data in the log without
// needing to query Redis or decode protobufs manually.
//
// It is intentionally brief: one log line per entity showing whichever of
// TripUpdate / VehiclePosition / Alert is populated.
func logFeedSample(key string, msg *gtfsrt.FeedMessage) {
	const maxSample = 3

	entities := msg.GetEntity()
	n := len(entities)
	if n == 0 {
		return
	}

	// Cap to maxSample without allocating a new slice.
	show := n
	if show > maxSample {
		show = maxSample
	}

	log.Printf("gtfs-rt: sample [%s] (showing %d of %d entities):", key, show, n)

	for _, e := range entities[:show] {
		id := e.GetId()

		switch {
		case e.TripUpdate != nil:
			tu := e.GetTripUpdate()
			trip := tu.GetTrip()
			delay := int32(0)
			// Use the first StopTimeUpdate's departure delay as a proxy for
			// overall schedule adherence.
			if stu := tu.GetStopTimeUpdate(); len(stu) > 0 {
				if dep := stu[0].GetDeparture(); dep != nil {
					delay = dep.GetDelay()
				}
			}
			log.Printf("  [%s] TripUpdate  trip=%s route=%s delay=%+ds stops=%d",
				id,
				trip.GetTripId(),
				trip.GetRouteId(),
				delay,
				len(tu.GetStopTimeUpdate()),
			)

		case e.Vehicle != nil:
			vp := e.GetVehicle()
			pos := vp.GetPosition()
			trip := vp.GetTrip()
			log.Printf("  [%s] VehiclePos  trip=%s route=%s lat=%.5f lon=%.5f bearing=%.1f speed=%.1fm/s",
				id,
				trip.GetTripId(),
				trip.GetRouteId(),
				pos.GetLatitude(),
				pos.GetLongitude(),
				pos.GetBearing(),
				pos.GetSpeed(),
			)

		case e.Alert != nil:
			al := e.GetAlert()
			cause := al.GetCause().String()
			effect := al.GetEffect().String()
			header := ""
			if tt := al.GetHeaderText(); tt != nil {
				if tr := tt.GetTranslation(); len(tr) > 0 {
					header = tr[0].GetText()
				}
			}
			log.Printf("  [%s] Alert       cause=%s effect=%s header=%q",
				id, cause, effect, header,
			)

		default:
			log.Printf("  [%s] (unknown entity type)", id)
		}
	}
}