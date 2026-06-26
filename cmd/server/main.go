// Command server is the entry point for the Route transit engine.
// On startup it parses the GTFS static feed, prints a summary of what was
// loaded, and reports how long the parse took. In a production build this is
// where you would initialise the HTTP server and hand the TransitGraph to the
// routing handlers.
package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"route/internal/gtfs"
)

func main() {
	const feedPath = "data/gtfs.zip"

	// Confirm the feed file is accessible before timing begins so that a
	// missing-file error is not attributed to parse latency.
	if _, err := os.Stat(feedPath); err != nil {
		log.Fatalf("feed file not found: %v", err)
	}

	fmt.Printf("Route — GTFS Static Parser\n")
	fmt.Printf("Loading feed: %s\n\n", feedPath)

	start := time.Now()

	graph, err := gtfs.ParseGTFS(feedPath)
	if err != nil {
		log.Fatalf("ParseGTFS failed: %v", err)
	}

	elapsed := time.Since(start)

	// Count total stop-time events across all trips for an extra data point.
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
}
