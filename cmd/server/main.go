// Command server is the entry point for the Route transit engine.
//
// Startup sequence:
//  1. Load .env so TFNSW_API_KEY and REDIS_* vars are available to all packages.
//  2. Parse the GTFS static feed into a TransitGraph held in memory.
//  3. Initialise the Redis client and verify connectivity.
//  4. Spawn the GTFS-RT polling worker as a background goroutine.
//  5. (Future) Bind the HTTP routing server and serve requests.
//
// The process listens for SIGINT / SIGTERM; on receipt it cancels the root
// context and waits for the RT worker to drain before exiting.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"route/internal/gtfs"
)

func main() {
	// ── 1. Environment ────────────────────────────────────────────────────────
	//
	// Load .env before any package reads os.Getenv so that TFNSW_API_KEY,
	// REDIS_ADDR, etc. are visible to the whole process.
	//
	// godotenv.Load does not overwrite variables that are already set in the
	// host environment, making it safe for both local dev (.env file present)
	// and container deployments (vars injected by the orchestrator).
	if err := godotenv.Load(); err != nil {
		// A missing .env is expected in production where env vars are injected
		// externally; degrade to a warning rather than a fatal error.
		log.Printf("server: .env not loaded — assuming environment is already set (%v)", err)
	}

	// ── 2. Static GTFS parse ──────────────────────────────────────────────────
	const feedPath = "data/gtfs.zip"

	if _, err := os.Stat(feedPath); err != nil {
		log.Fatalf("server: feed file not found: %v", err)
	}

	fmt.Printf("Route — GTFS Static Parser\n")
	fmt.Printf("Loading feed: %s\n\n", feedPath)

	parseStart := time.Now()

	graph, err := gtfs.ParseGTFS(feedPath)
	if err != nil {
		log.Fatalf("server: ParseGTFS failed: %v", err)
	}

	elapsed := time.Since(parseStart)

	// Count total stop-time events across all trips for the startup summary.
	totalStopTimes := 0
	for _, trip := range graph.Trips {
		totalStopTimes += len(trip.StopTimes)
	}

	fmt.Printf("Parse complete in %d ms\n", elapsed.Milliseconds())
	fmt.Printf("  Stops       : %d\n", len(graph.Stops))
	fmt.Printf("  Routes      : %d\n", len(graph.Routes))
	fmt.Printf("  Trips       : %d\n", len(graph.Trips))
	fmt.Printf("  Stop events : %d\n", totalStopTimes)

	// Spot-check: print the first stop alphabetically just to prove data is
	// live and not a compilation artifact.
	for id, s := range graph.Stops {
		fmt.Printf("\nSample stop  : id=%q lat=%.6f lon=%.6f\n", id, s.Lat, s.Lon)
		break
	}
	fmt.Println()

	// ── 3. Redis ──────────────────────────────────────────────────────────────
	rdb, err := gtfs.InitRedis()
	if err != nil {
		log.Fatalf("server: redis init failed: %v", err)
	}
	defer rdb.Close()

	// ── 4. Graceful shutdown context ──────────────────────────────────────────
	//
	// A root context that is cancelled when the OS sends SIGINT or SIGTERM.
	// Every long-lived subsystem (RT worker, future HTTP server) receives this
	// context so shutdown is coordinated from a single cancellation point.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── 5. GTFS-RT polling worker ─────────────────────────────────────────────
	//
	// The worker runs in its own goroutine and polls all TfNSW endpoints every
	// 15 seconds. It blocks until ctx is cancelled, at which point it finishes
	// the current round and returns — keeping Redis keys fresh right up to the
	// moment the process exits.
	go gtfs.StartRTWorker(ctx, rdb, 15*time.Second)

	// ── 6. Block until signal ─────────────────────────────────────────────────
	//
	// Wait here until SIGINT / SIGTERM arrives. In a future iteration this
	// select will also wait on the HTTP server's ListenAndServe error channel.
	<-ctx.Done()
	log.Println("server: shutdown signal received — exiting")
}
