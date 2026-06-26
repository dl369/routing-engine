// Package gtfs implements a high-performance parser for GTFS Static feeds.
//
// Design goals:
//   - Zero external dependencies — only stdlib packages.
//   - Pre-allocated slices to avoid repeated heap growth during parsing.
//   - Custom O(1) time parser that avoids the overhead of time.Parse and
//     correctly handles GTFS times beyond 24:00:00.
//   - Data is stored in flat structs (no pointer graphs) for cache efficiency.
package gtfs

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"route/internal/models"
)

// ParseGTFS opens the GTFS zip archive at zipPath, parses the four required
// feed files in dependency order, and returns a fully populated TransitGraph.
//
// Parsing order matters:
//  1. stops.txt      — no dependencies
//  2. routes.txt     — no dependencies
//  3. trips.txt      — depends on routes
//  4. stop_times.txt — depends on trips and stops
func ParseGTFS(zipPath string) (*models.TransitGraph, error) {
	// Open the zip archive. zip.OpenReader memory-maps the file on most
	// platforms, so we avoid loading the entire archive into RAM at once.
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("gtfs: open zip %q: %w", zipPath, err)
	}
	defer zr.Close()

	// Build a name → *zip.File index so each parser can locate its file in O(1)
	// rather than scanning the directory repeatedly.
	fileIndex := make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		fileIndex[f.Name] = f
	}

	graph := &models.TransitGraph{}

	if err := withZipEntry(fileIndex, "stops.txt", func(csvr *csv.Reader) error {
		return parseStops(csvr, graph)
	}); err != nil {
		return nil, err
	}
	if err := withZipEntry(fileIndex, "routes.txt", func(csvr *csv.Reader) error {
		return parseRoutes(csvr, graph)
	}); err != nil {
		return nil, err
	}
	if err := withZipEntry(fileIndex, "trips.txt", func(csvr *csv.Reader) error {
		return parseTrips(csvr, graph)
	}); err != nil {
		return nil, err
	}
	if err := withZipEntry(fileIndex, "stop_times.txt", func(csvr *csv.Reader) error {
		return parseStopTimes(csvr, graph)
	}); err != nil {
		return nil, err
	}

	// Sort every trip's stop-times slice by sequence number so the routing
	// algorithm can do a single forward scan without random access.
	for _, trip := range graph.Trips {
		sort.Slice(trip.StopTimes, func(i, j int) bool {
			return trip.StopTimes[i].Sequence < trip.StopTimes[j].Sequence
		})
	}

	return graph, nil
}

// ---------------------------------------------------------------------------
// File-level parsers
// ---------------------------------------------------------------------------

// parseStops reads stops.txt from csvr and populates graph.Stops.
// Required columns: stop_id, stop_lat, stop_lon.
func parseStops(csvr *csv.Reader, graph *models.TransitGraph) error {
	header, colStop, colLat, colLon, err := readHeader(csvr, "stops.txt",
		"stop_id", "stop_lat", "stop_lon")
	if err != nil {
		return err
	}
	_ = header

	// GTFS feeds commonly contain tens of thousands of stops; pre-allocate a
	// reasonable capacity to avoid repeated map rehashing.
	graph.Stops = make(map[string]*models.Stop, 8192)

	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: stops.txt: %w", err)
		}

		lat, err := strconv.ParseFloat(rec[colLat], 64)
		if err != nil {
			return fmt.Errorf("gtfs: stops.txt: invalid lat %q: %w", rec[colLat], err)
		}
		lon, err := strconv.ParseFloat(rec[colLon], 64)
		if err != nil {
			return fmt.Errorf("gtfs: stops.txt: invalid lon %q: %w", rec[colLon], err)
		}

		// Copy the stop_id string — rec is reused on the next Read call.
		id := string(rec[colStop])
		graph.Stops[id] = &models.Stop{ID: id, Lat: lat, Lon: lon}
	}
	return nil
}

// parseRoutes reads routes.txt from csvr and populates graph.Routes.
// Required columns: route_id, route_short_name, route_type.
func parseRoutes(csvr *csv.Reader, graph *models.TransitGraph) error {
	_, colID, colName, colType, err := readHeader(csvr, "routes.txt",
		"route_id", "route_short_name", "route_type")
	if err != nil {
		return err
	}

	graph.Routes = make(map[string]*models.Route, 512)

	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: routes.txt: %w", err)
		}

		routeType, err := strconv.Atoi(rec[colType])
		if err != nil {
			return fmt.Errorf("gtfs: routes.txt: invalid route_type %q: %w", rec[colType], err)
		}

		id := string(rec[colID])
		graph.Routes[id] = &models.Route{
			ID:        id,
			ShortName: string(rec[colName]),
			Type:      routeType,
		}
	}
	return nil
}

// parseTrips reads trips.txt from csvr and populates graph.Trips with empty StopTimes
// slices that will be filled by parseStopTimes.
// Required columns: trip_id, route_id.
func parseTrips(csvr *csv.Reader, graph *models.TransitGraph) error {
	_, colTrip, colRoute, _, err := readHeader(csvr, "trips.txt",
		"trip_id", "route_id")
	if err != nil {
		return err
	}

	graph.Trips = make(map[string]*models.Trip, 32768)

	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: trips.txt: %w", err)
		}

		id := string(rec[colTrip])
		graph.Trips[id] = &models.Trip{
			ID:      id,
			RouteID: string(rec[colRoute]),
			// Pre-allocate for a typical number of stops per trip (≈30) so the
			// append calls in parseStopTimes rarely need to grow the slice.
			StopTimes: make([]models.StopTime, 0, 32),
		}
	}
	return nil
}

// parseStopTimes reads stop_times.txt from csvr — typically the largest file in a
// GTFS feed — and appends each event to the corresponding Trip.
// Required columns: trip_id, stop_id, arrival_time, departure_time, stop_sequence.
func parseStopTimes(csvr *csv.Reader, graph *models.TransitGraph) error {
	_, colTrip, colStop, colArr, colDep, colSeq, err := readHeader5(csvr, "stop_times.txt",
		"trip_id", "stop_id", "arrival_time", "departure_time", "stop_sequence")
	if err != nil {
		return err
	}

	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: stop_times.txt: %w", err)
		}

		trip, ok := graph.Trips[rec[colTrip]]
		if !ok {
			// Orphaned stop_time — skip rather than hard-error; some feeds
			// include test/deleted trips that were pruned from trips.txt.
			continue
		}

		arr, err := parseGTFSTime(rec[colArr])
		if err != nil {
			return fmt.Errorf("gtfs: stop_times.txt: arrival_time %q: %w", rec[colArr], err)
		}
		dep, err := parseGTFSTime(rec[colDep])
		if err != nil {
			return fmt.Errorf("gtfs: stop_times.txt: departure_time %q: %w", rec[colDep], err)
		}
		seq, err := strconv.ParseInt(rec[colSeq], 10, 32)
		if err != nil {
			return fmt.Errorf("gtfs: stop_times.txt: stop_sequence %q: %w", rec[colSeq], err)
		}

		trip.StopTimes = append(trip.StopTimes, models.StopTime{
			StopID:        string(rec[colStop]),
			Sequence:      int32(seq),
			ArrivalTime:   arr,
			DepartureTime: dep,
		})
	}
	return nil
}

// ---------------------------------------------------------------------------
// Custom time parser
// ---------------------------------------------------------------------------

// parseGTFSTime converts a GTFS time string ("HH:MM:SS") to an int32
// representing total seconds past midnight.
//
// Why not time.Parse?
//  1. Performance: time.Parse allocates and does locale/timezone work we don't
//     need. This function is called millions of times for large feeds.
//  2. Correctness: GTFS explicitly allows hours ≥ 24 for services that run
//     past midnight on the service day (e.g. "25:30:00"). time.Parse rejects
//     these values; this parser handles them naturally.
//
// The input must be exactly "H…H:MM:SS" with two-digit minutes and seconds.
// Returns an error only if the string is structurally invalid.
func parseGTFSTime(s string) (int32, error) {
	// Minimum valid length is "0:00:00" (7 chars); typical is "HH:MM:SS" (8).
	if len(s) < 7 {
		return 0, fmt.Errorf("too short: %q", s)
	}

	// Locate the two colon separators. We scan from the right so that a
	// variable-width hours field (e.g. "9:00:00" vs "25:00:00") is handled
	// without branching on hour width.
	lastColon := len(s) - 3 // colon before SS
	if s[lastColon] != ':' {
		return 0, fmt.Errorf("missing second colon in %q", s)
	}
	midColon := lastColon - 3 // colon before MM
	if midColon < 0 || s[midColon] != ':' {
		return 0, fmt.Errorf("missing first colon in %q", s)
	}

	// Parse each component as plain ASCII digits — no allocations.
	hours, err := atoiBytes(s[:midColon])
	if err != nil {
		return 0, fmt.Errorf("invalid hours in %q: %w", s, err)
	}
	minutes, err := atoiBytes(s[midColon+1 : lastColon])
	if err != nil {
		return 0, fmt.Errorf("invalid minutes in %q: %w", s, err)
	}
	seconds, err := atoiBytes(s[lastColon+1:])
	if err != nil {
		return 0, fmt.Errorf("invalid seconds in %q: %w", s, err)
	}

	if minutes > 59 || seconds > 59 {
		return 0, fmt.Errorf("out-of-range time components in %q", s)
	}

	return int32(hours*3600 + minutes*60 + seconds), nil
}

// atoiBytes converts a small ASCII digit string to an int without allocating.
// It is a tight loop equivalent to strconv.Atoi but operates on a string slice
// already pointing into the CSV record buffer.
func atoiBytes(s string) (int, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty number string")
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("non-digit character %q", c)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// CSV header helpers
// ---------------------------------------------------------------------------

// withZipEntry opens name from the zip index, wraps it in a csv.Reader, passes
// that to fn, and closes the underlying stream when done.
func withZipEntry(idx map[string]*zip.File, name string, fn func(*csv.Reader) error) error {
	rc, err := openFile(idx, name)
	if err != nil {
		return err
	}
	defer rc.Close()
	csvr := csv.NewReader(rc)
	csvr.ReuseRecord = true // reuse the backing array between Read calls
	return fn(csvr)
}

// openFile looks up name in the zip index and returns a ReadCloser.
func openFile(idx map[string]*zip.File, name string) (io.ReadCloser, error) {
	f, ok := idx[name]
	if !ok {
		return nil, fmt.Errorf("gtfs: %q not found in zip", name)
	}
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("gtfs: open %q: %w", name, err)
	}
	return rc, nil
}

// readHeader reads the first (header) row of a CSV and returns the zero-based
// column indices for up to three named columns. This variadic-style approach
// keeps the column-lookup logic in one place and lets each parser call it with
// only the columns it actually needs.
//
// Returns: header row, col0, col1, col2, error.
// col2 is -1 when only two column names are requested.
func readHeader(r *csv.Reader, filename string, cols ...string) ([]string, int, int, int, error) {
	header, err := r.Read()
	if err != nil {
		return nil, -1, -1, -1, fmt.Errorf("gtfs: %s: read header: %w", filename, err)
	}

	index := make(map[string]int, len(header))
	for i, h := range header {
		index[trimBOM(h)] = i
	}

	get := func(name string) (int, error) {
		i, ok := index[name]
		if !ok {
			return -1, fmt.Errorf("gtfs: %s: missing column %q", filename, name)
		}
		return i, nil
	}

	c0, err := get(cols[0])
	if err != nil {
		return nil, -1, -1, -1, err
	}
	c1, err := get(cols[1])
	if err != nil {
		return nil, -1, -1, -1, err
	}
	c2 := -1
	if len(cols) >= 3 {
		c2, err = get(cols[2])
		if err != nil {
			return nil, -1, -1, -1, err
		}
	}
	return header, c0, c1, c2, nil
}

// readHeader5 is the five-column variant used by parseStopTimes.
// Returns: header row, col0…col4, error.
func readHeader5(r *csv.Reader, filename string, cols ...string) ([]string, int, int, int, int, int, error) {
	header, err := r.Read()
	if err != nil {
		return nil, -1, -1, -1, -1, -1, fmt.Errorf("gtfs: %s: read header: %w", filename, err)
	}

	index := make(map[string]int, len(header))
	for i, h := range header {
		index[trimBOM(h)] = i
	}

	get := func(name string) (int, error) {
		i, ok := index[name]
		if !ok {
			return -1, fmt.Errorf("gtfs: %s: missing required column %q", filename, name)
		}
		return i, nil
	}

	c0, err := get(cols[0])
	if err != nil {
		return nil, -1, -1, -1, -1, -1, err
	}
	c1, err := get(cols[1])
	if err != nil {
		return nil, -1, -1, -1, -1, -1, err
	}
	c2, err := get(cols[2])
	if err != nil {
		return nil, -1, -1, -1, -1, -1, err
	}
	c3, err := get(cols[3])
	if err != nil {
		return nil, -1, -1, -1, -1, -1, err
	}
	c4, err := get(cols[4])
	if err != nil {
		return nil, -1, -1, -1, -1, -1, err
	}

	return header, c0, c1, c2, c3, c4, nil
}

// trimBOM strips the UTF-8 byte-order mark that some GTFS producers prepend to
// the first field of the first line (a common Excel/Windows artifact).
func trimBOM(s string) string {
	if len(s) >= 3 && s[0] == 0xEF && s[1] == 0xBB && s[2] == 0xBF {
		return s[3:]
	}
	return s
}
