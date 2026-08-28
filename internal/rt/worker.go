package rt

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"route/internal/gtfs"
)

// StartLoader runs a background loop that reads protobuf payloads from Redis,
// unmarshals them, and publishes entity-slice snapshots every interval.
//
// Loads all feeds immediately on startup, then on each tick. Run in a goroutine:
//
//	go rt.StartLoader(ctx, rdb, cache, 15*time.Second)
func StartLoader(ctx context.Context, rdb *redis.Client, cache *Cache, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("rt: loader started — interval=%v feeds=%d", interval, len(gtfs.RTFeedKeys()))

	loadAll(ctx, rdb, cache)

	for {
		select {
		case <-ctx.Done():
			log.Printf("rt: loader stopping: %v", ctx.Err())
			return
		case <-ticker.C:
			loadAll(ctx, rdb, cache)
		}
	}
}

// loadAll fetches every GTFS-RT key from Redis concurrently, then publishes
// one atomic snapshot merge so readers never observe a partially updated cache.
func loadAll(ctx context.Context, rdb *redis.Client, cache *Cache) {
	keys := gtfs.RTFeedKeys()

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex // guards updates map during fan-out only
		updates      = make(map[string][]Entity, len(keys))
		successCount int
		errCount     int
	)

	for _, key := range keys {
		key := key
		wg.Add(1)

		go func() {
			defer wg.Done()

			entities, err := fetchEntities(ctx, rdb, key)
			if err != nil {
				log.Printf("rt: load error [%s]: %v", key, err)
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}

			mu.Lock()
			updates[key] = entities
			successCount++
			mu.Unlock()

			log.Printf("rt: fetched %s — entities=%d", key, len(entities))
		}()
	}

	wg.Wait()
	cache.Merge(updates)
	log.Printf("rt: load complete — ok=%d err=%d cached=%d", successCount, errCount, cache.Len())
}

// fetchEntities GETs raw protobuf from Redis and returns a new entity slice.
func fetchEntities(ctx context.Context, rdb *redis.Client, key string) ([]Entity, error) {
	raw, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, fmt.Errorf("redis GET: %w", err)
	}

	var msg gtfsrt.FeedMessage
	if err := proto.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %w", err)
	}

	return FromFeedMessage(&msg), nil
}
