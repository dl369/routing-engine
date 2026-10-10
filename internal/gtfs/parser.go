// Package gtfs implements a high-performance parser for GTFS Static feeds.
//
// Design goals:
//   - Zero external dependencies — only stdlib packages.
//   - Pointer-free columnar Feed: numeric columns hold no GC-scanned pointers.
//   - strings.Clone on every retained ID so csv.Reader row buffers are not pinned.
//   - Parallel parse of independent zip entries (stops / routes / services).
//   - Custom O(1) time parser that avoids time.Parse and accepts hours 0–47.
//
// The Feed is a one-shot staging set for router.Build; it is not the hot path.
package gtfs

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"route/internal/models"
)

// ParseGTFS opens the GTFS zip archive at zipPath and returns a columnar Feed.
// It opens the file as a ReaderAt and delegates to ParseGTFSReader.
func ParseGTFS(zipPath string) (*models.Feed, error) {
	f, err := os.Open(zipPath)
	if err != nil {
		return nil, fmt.Errorf("gtfs: open zip %q: %w", zipPath, err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("gtfs: stat zip %q: %w", zipPath, err)
	}
	return ParseGTFSReader(f, st.Size())
}

// ParseGTFSReader parses a GTFS zip from an io.ReaderAt (used by tests with
// in-memory fixtures, and by ParseGTFS after opening the on-disk archive).
//
// Stages (barriers between them — each goroutine owns a disjoint Feed region):
//
//	A — concurrently: stops | routes | services (calendar then calendar_dates)
//	B — trips (needs route + service maps)
//	C — stop_times (needs trip + stop maps)
func ParseGTFSReader(ra io.ReaderAt, size int64) (*models.Feed, error) {
	// zip.NewReader reads the central directory via ReaderAt; individual
	// entries are then inflated through deflate on Open — there is no mmap.
	zr, err := zip.NewReader(ra, size)
	if err != nil {
		return nil, fmt.Errorf("gtfs: open zip reader: %w", err)
	}

	// Resolve every entry up front so a missing required file fails before
	// any parsing work is done.
	ff, err := locateFiles(zr)
	if err != nil {
		return nil, err
	}

	feed := &models.Feed{
		Index: models.FeedIndex{
			Stops:    make(map[string]uint32),
			Routes:   make(map[string]uint32),
			Trips:    make(map[string]uint32),
			Services: make(map[string]uint32),
		},
	}

	// ── Stage A ──────────────────────────────────────────────────────────────
	err = runParallel(
		func() error { return parseStops(ff.stops, feed) },
		func() error { return parseRoutes(ff.routes, feed) },
		func() error {
			// calendar + calendar_dates share the service intern map — sequential.
			// locateFiles guarantees at least one of them is present.
			if ff.calendar != nil {
				if err := parseCalendar(ff.calendar, feed); err != nil {
					return err
				}
			}
			if ff.calendarDates != nil {
				return parseCalendarDates(ff.calendarDates, feed)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	// ── Stage B ──────────────────────────────────────────────────────────────
	if err := parseTrips(ff.trips, feed); err != nil {
		return nil, err
	}

	// ── Stage C ──────────────────────────────────────────────────────────────
	if err := parseStopTimes(ff.stopTimes, feed); err != nil {
		return nil, err
	}

	return feed, nil
}

// ---------------------------------------------------------------------------
// Orchestration helpers
// ---------------------------------------------------------------------------

// runParallel runs fns concurrently and returns the first non-nil error in
// call order after all have finished (stdlib WaitGroup — no x/sync/errgroup).
func runParallel(fns ...func() error) error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	wg.Add(len(fns))
	for i, fn := range fns {
		go func(i int, fn func() error) {
			defer wg.Done()
			errs[i] = fn()
		}(i, fn)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// feedFiles holds the zip entries the parser reads. Required entries are
// non-nil after locateFiles succeeds; optional entries are nil when absent.
type feedFiles struct {
	stops, routes, trips, stopTimes *zip.File // required
	calendar, calendarDates         *zip.File // optional (at least one present)
}

// locateFiles finds every GTFS entry the parser needs in a single pass over
// the zip directory. Entries are matched by base name so feeds zipped with a
// top-level folder (e.g. "gtfs/stops.txt") are accepted.
func locateFiles(zr *zip.Reader) (feedFiles, error) {
	var ff feedFiles
	for _, f := range zr.File {
		switch path.Base(f.Name) {
		case "stops.txt":
			ff.stops = f
		case "routes.txt":
			ff.routes = f
		case "trips.txt":
			ff.trips = f
		case "stop_times.txt":
			ff.stopTimes = f
		case "calendar.txt":
			ff.calendar = f
		case "calendar_dates.txt":
			ff.calendarDates = f
		}
	}

	switch {
	case ff.stops == nil:
		return ff, errors.New("gtfs: stops.txt not found in zip")
	case ff.routes == nil:
		return ff, errors.New("gtfs: routes.txt not found in zip")
	case ff.trips == nil:
		return ff, errors.New("gtfs: trips.txt not found in zip")
	case ff.stopTimes == nil:
		return ff, errors.New("gtfs: stop_times.txt not found in zip")
	case ff.calendar == nil && ff.calendarDates == nil:
		return ff, errors.New("gtfs: both calendar.txt and calendar_dates.txt are missing")
	}
	return ff, nil
}

// openCSV opens a zip entry and wraps it in a csv.Reader configured for GTFS.
// The caller must Close the returned io.Closer. zip.File.Open is safe
// concurrently on different entries (each is a SectionReader over the shared
// ReaderAt).
func openCSV(f *zip.File) (*csv.Reader, io.Closer, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, nil, fmt.Errorf("gtfs: open %q: %w", f.Name, err)
	}

	// 1 MiB read-ahead over deflate.
	r := csv.NewReader(bufio.NewReaderSize(rc, 1<<20))
	// Reuse the []string slice between rows; fields still share one row string.
	r.ReuseRecord = true
	// Tolerate trailing / missing columns; parsers read through field().
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	return r, rc, nil
}

// ---------------------------------------------------------------------------
// File-level parsers
// ---------------------------------------------------------------------------

// parentPending records a stop whose parent_station was not yet in the index
// (forward reference). parentID is Clone'd so the CSV row is not pinned.
type parentPending struct {
	child    uint32
	parentID string
}

// parseStops reads stops.txt into the stop columns and Index.Stops.
// Required: stop_id. Optional: stop_name, stop_lat, stop_lon, location_type, parent_station.
func parseStops(f *zip.File, feed *models.Feed) error {
	r, c, err := openCSV(f)
	if err != nil {
		return err
	}
	defer c.Close()
	size := f.UncompressedSize64

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: stops.txt: read header: %w", err)
	}
	req, opt, err := columns(header, "stops.txt",
		[]string{"stop_id"},
		[]string{"stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"},
	)
	if err != nil {
		return err
	}
	colID := req[0]
	colName, colLat, colLon, colLoc, colParent := opt[0], opt[1], opt[2], opt[3], opt[4]

	n := int(size / 150)
	if n < 64 {
		n = 64
	}
	feed.StopIDs = make([]string, 0, n)
	feed.StopNames = make([]string, 0, n)
	feed.StopLat = make([]float64, 0, n)
	feed.StopLon = make([]float64, 0, n)
	feed.StopLocType = make([]uint8, 0, n)
	feed.StopParent = make([]uint32, 0, n)
	feed.Index.Stops = make(map[string]uint32, n)

	pending := make([]parentPending, 0, n/4)

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: stops.txt: %w", err)
		}
		line, _ := r.FieldPos(0)

		rawID := field(rec, colID)
		if rawID == "" {
			return fmt.Errorf("gtfs: stops.txt:%d: empty stop_id", line)
		}
		if _, exists := feed.Index.Stops[rawID]; exists {
			feed.Stats.DuplicateIDs++
			continue
		}

		locType, err := parseEnum(field(rec, colLoc), 4, 0)
		if err != nil {
			return fmt.Errorf("gtfs: stops.txt:%d: location_type %q: %w", line, field(rec, colLoc), err)
		}

		latStr := strings.TrimSpace(field(rec, colLat))
		lonStr := strings.TrimSpace(field(rec, colLon))
		var lat, lon float64
		switch {
		case locType <= 2:
			if latStr == "" || lonStr == "" {
				return fmt.Errorf("gtfs: stops.txt:%d: stop_lat/stop_lon required for location_type %d", line, locType)
			}
			lat, err = strconv.ParseFloat(latStr, 64)
			if err != nil {
				return fmt.Errorf("gtfs: stops.txt:%d: invalid stop_lat %q: %w", line, latStr, err)
			}
			lon, err = strconv.ParseFloat(lonStr, 64)
			if err != nil {
				return fmt.Errorf("gtfs: stops.txt:%d: invalid stop_lon %q: %w", line, lonStr, err)
			}
		default:
			// location_type 3/4 (generic node / boarding area): coords optional.
			if latStr == "" || lonStr == "" {
				lat, lon = math.NaN(), math.NaN()
			} else {
				lat, err = strconv.ParseFloat(latStr, 64)
				if err != nil {
					return fmt.Errorf("gtfs: stops.txt:%d: invalid stop_lat %q: %w", line, latStr, err)
				}
				lon, err = strconv.ParseFloat(lonStr, 64)
				if err != nil {
					return fmt.Errorf("gtfs: stops.txt:%d: invalid stop_lon %q: %w", line, lonStr, err)
				}
			}
		}

		id := strings.Clone(rawID)
		name := strings.Clone(field(rec, colName))
		idx := uint32(len(feed.StopIDs))
		feed.StopIDs = append(feed.StopIDs, id)
		feed.StopNames = append(feed.StopNames, name)
		feed.StopLat = append(feed.StopLat, lat)
		feed.StopLon = append(feed.StopLon, lon)
		feed.StopLocType = append(feed.StopLocType, locType)
		feed.StopParent = append(feed.StopParent, models.NoIndex)
		feed.Index.Stops[id] = idx

		parentRaw := field(rec, colParent)
		if parentRaw != "" {
			if pidx, ok := feed.Index.Stops[parentRaw]; ok {
				feed.StopParent[idx] = pidx
			} else {
				pending = append(pending, parentPending{child: idx, parentID: strings.Clone(parentRaw)})
			}
		}
	}

	for _, p := range pending {
		if pidx, ok := feed.Index.Stops[p.parentID]; ok {
			feed.StopParent[p.child] = pidx
		} else {
			feed.StopParent[p.child] = models.NoIndex
			feed.Stats.ParentUnresolved++
		}
	}
	return nil
}

// parseRoutes reads routes.txt into the route columns and Index.Routes.
// Required: route_id, route_type. Optional: route_short_name.
func parseRoutes(f *zip.File, feed *models.Feed) error {
	r, c, err := openCSV(f)
	if err != nil {
		return err
	}
	defer c.Close()

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: routes.txt: read header: %w", err)
	}
	req, opt, err := columns(header, "routes.txt",
		[]string{"route_id", "route_type"},
		[]string{"route_short_name"},
	)
	if err != nil {
		return err
	}
	colID, colType := req[0], req[1]
	colName := opt[0]

	feed.RouteIDs = make([]string, 0, 512)
	feed.RouteShortNames = make([]string, 0, 512)
	feed.RouteTypes = make([]uint16, 0, 512)
	feed.Index.Routes = make(map[string]uint32, 512)

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: routes.txt: %w", err)
		}
		line, _ := r.FieldPos(0)

		rawID := field(rec, colID)
		if rawID == "" {
			return fmt.Errorf("gtfs: routes.txt:%d: empty route_id", line)
		}
		if _, exists := feed.Index.Routes[rawID]; exists {
			continue // first wins; DuplicateIDs is reserved for stops/trips
		}

		rt, err := parseUint32(field(rec, colType))
		if err != nil {
			return fmt.Errorf("gtfs: routes.txt:%d: route_type %q: %w", line, field(rec, colType), err)
		}
		if rt > math.MaxUint16 {
			return fmt.Errorf("gtfs: routes.txt:%d: route_type %d exceeds uint16", line, rt)
		}

		id := strings.Clone(rawID)
		idx := uint32(len(feed.RouteIDs))
		feed.RouteIDs = append(feed.RouteIDs, id)
		feed.RouteShortNames = append(feed.RouteShortNames, strings.Clone(field(rec, colName)))
		feed.RouteTypes = append(feed.RouteTypes, uint16(rt))
		feed.Index.Routes[id] = idx
	}
	return nil
}

// parseCalendar reads calendar.txt into Calendars and interns service IDs.
func parseCalendar(f *zip.File, feed *models.Feed) error {
	r, c, err := openCSV(f)
	if err != nil {
		return err
	}
	defer c.Close()

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: calendar.txt: read header: %w", err)
	}
	req, _, err := columns(header, "calendar.txt",
		[]string{"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"},
		nil,
	)
	if err != nil {
		return err
	}
	colSvc := req[0]
	colMon, colTue, colWed, colThu := req[1], req[2], req[3], req[4]
	colFri, colSat, colSun := req[5], req[6], req[7]
	colStart, colEnd := req[8], req[9]

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: calendar.txt: %w", err)
		}
		line, _ := r.FieldPos(0)

		svcIdx, err := internService(feed, field(rec, colSvc))
		if err != nil {
			return fmt.Errorf("gtfs: calendar.txt:%d: service_id: %w", line, err)
		}

		var wd uint8
		days := [...]struct {
			col int
			bit uint8
		}{
			{colMon, models.Monday},
			{colTue, models.Tuesday},
			{colWed, models.Wednesday},
			{colThu, models.Thursday},
			{colFri, models.Friday},
			{colSat, models.Saturday},
			{colSun, models.Sunday},
		}
		for _, d := range days {
			v, err := parseDayFlag(field(rec, d.col))
			if err != nil {
				return fmt.Errorf("gtfs: calendar.txt:%d: weekday flag: %w", line, err)
			}
			if v == 1 {
				wd |= d.bit
			}
		}

		start, err := parseDate(field(rec, colStart))
		if err != nil {
			return fmt.Errorf("gtfs: calendar.txt:%d: start_date %q: %w", line, field(rec, colStart), err)
		}
		end, err := parseDate(field(rec, colEnd))
		if err != nil {
			return fmt.Errorf("gtfs: calendar.txt:%d: end_date %q: %w", line, field(rec, colEnd), err)
		}
		if start > end {
			return fmt.Errorf("gtfs: calendar.txt:%d: start_date %d > end_date %d", line, start, end)
		}

		feed.Calendars = append(feed.Calendars, models.Calendar{
			Service:  svcIdx,
			Weekdays: wd,
			Start:    start,
			End:      end,
		})
	}
	return nil
}

// parseCalendarDates reads calendar_dates.txt into CalendarDates.
// Unknown services are interned here (exception-only services).
func parseCalendarDates(f *zip.File, feed *models.Feed) error {
	r, c, err := openCSV(f)
	if err != nil {
		return err
	}
	defer c.Close()

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: calendar_dates.txt: read header: %w", err)
	}
	req, _, err := columns(header, "calendar_dates.txt",
		[]string{"service_id", "date", "exception_type"},
		nil,
	)
	if err != nil {
		return err
	}
	colSvc, colDate, colExc := req[0], req[1], req[2]

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: calendar_dates.txt: %w", err)
		}
		line, _ := r.FieldPos(0)

		svcIdx, err := internService(feed, field(rec, colSvc))
		if err != nil {
			return fmt.Errorf("gtfs: calendar_dates.txt:%d: service_id: %w", line, err)
		}
		date, err := parseDate(field(rec, colDate))
		if err != nil {
			return fmt.Errorf("gtfs: calendar_dates.txt:%d: date %q: %w", line, field(rec, colDate), err)
		}
		exc, err := parseUint32(field(rec, colExc))
		if err != nil {
			return fmt.Errorf("gtfs: calendar_dates.txt:%d: exception_type %q: %w", line, field(rec, colExc), err)
		}
		if exc != 1 && exc != 2 {
			return fmt.Errorf("gtfs: calendar_dates.txt:%d: exception_type %d (want 1 or 2)", line, exc)
		}

		feed.CalendarDates = append(feed.CalendarDates, models.CalendarDate{
			Service: svcIdx,
			Date:    date,
			Added:   exc == 1,
		})
	}
	return nil
}

// parseTrips reads trips.txt into trip columns. Needs route and service maps.
// Required: trip_id, route_id, service_id.
func parseTrips(f *zip.File, feed *models.Feed) error {
	r, c, err := openCSV(f)
	if err != nil {
		return err
	}
	defer c.Close()
	size := f.UncompressedSize64

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: trips.txt: read header: %w", err)
	}
	req, _, err := columns(header, "trips.txt",
		[]string{"trip_id", "route_id", "service_id"},
		nil,
	)
	if err != nil {
		return err
	}
	colTrip, colRoute, colSvc := req[0], req[1], req[2]

	n := int(size / 80)
	if n < 64 {
		n = 64
	}
	feed.TripIDs = make([]string, 0, n)
	feed.TripRoute = make([]uint32, 0, n)
	feed.TripService = make([]uint32, 0, n)
	feed.Index.Trips = make(map[string]uint32, n)

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: trips.txt: %w", err)
		}
		line, _ := r.FieldPos(0)

		rawID := field(rec, colTrip)
		if rawID == "" {
			return fmt.Errorf("gtfs: trips.txt:%d: empty trip_id", line)
		}
		if _, exists := feed.Index.Trips[rawID]; exists {
			feed.Stats.DuplicateIDs++
			continue
		}

		routeIdx := models.NoIndex
		rawRoute := field(rec, colRoute)
		if ridx, ok := feed.Index.Routes[rawRoute]; ok {
			routeIdx = ridx
		} else {
			feed.Stats.TripsUnknownRoute++
		}

		rawSvc := field(rec, colSvc)
		svcIdx, ok := feed.Index.Services[rawSvc]
		if !ok {
			// Intern so the trip has a dense service index; without a calendar
			// row it will never be active on any date.
			var err error
			svcIdx, err = internService(feed, rawSvc)
			if err != nil {
				return fmt.Errorf("gtfs: trips.txt:%d: service_id: %w", line, err)
			}
			feed.Stats.TripsUnknownService++
		}

		id := strings.Clone(rawID)
		idx := uint32(len(feed.TripIDs))
		feed.TripIDs = append(feed.TripIDs, id)
		feed.TripRoute = append(feed.TripRoute, routeIdx)
		feed.TripService = append(feed.TripService, svcIdx)
		feed.Index.Trips[id] = idx
	}
	return nil
}

// parseStopTimes reads stop_times.txt into parallel ST* columns (file order).
// No sorting — router.Build will counting-sort by (trip, seq).
//
// Hot path: no fmt/strconv/closures on success. Last-trip cache avoids a map
// lookup on consecutive rows of the same trip (common in TfNSW ordering).
func parseStopTimes(f *zip.File, feed *models.Feed) error {
	r, c, err := openCSV(f)
	if err != nil {
		return err
	}
	defer c.Close()
	size := f.UncompressedSize64

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: stop_times.txt: read header: %w", err)
	}
	req, opt, err := columns(header, "stop_times.txt",
		[]string{"trip_id", "stop_id", "stop_sequence"},
		[]string{"arrival_time", "departure_time", "pickup_type", "drop_off_type"},
	)
	if err != nil {
		return err
	}
	colTrip, colStop, colSeq := req[0], req[1], req[2]
	colArr, colDep, colPick, colDrop := opt[0], opt[1], opt[2], opt[3]

	n := int(size / 40)
	if n < 64 {
		n = 64
	}
	feed.STTrip = make([]uint32, 0, n)
	feed.STStop = make([]uint32, 0, n)
	feed.STSeq = make([]uint32, 0, n)
	feed.STArr = make([]int32, 0, n)
	feed.STDep = make([]int32, 0, n)
	feed.STPickup = make([]uint8, 0, n)
	feed.STDrop = make([]uint8, 0, n)

	var (
		lastTripID  string
		lastTripIdx uint32
		haveLast    bool
	)

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: stop_times.txt: %w", err)
		}

		rawTrip := field(rec, colTrip)
		var tripIdx uint32
		if haveLast && rawTrip == lastTripID {
			tripIdx = lastTripIdx
		} else {
			idx, ok := feed.Index.Trips[rawTrip]
			if !ok {
				feed.Stats.StopTimesUnknownTrip++
				continue
			}
			tripIdx = idx
			// Use the map's cloned key (TripIDs[idx]), never the row substring.
			lastTripID = feed.TripIDs[idx]
			lastTripIdx = idx
			haveLast = true
		}

		rawStop := field(rec, colStop)
		stopIdx, ok := feed.Index.Stops[rawStop]
		if !ok {
			feed.Stats.StopTimesUnknownStop++
			continue
		}

		seq, err := parseUint32(field(rec, colSeq))
		if err != nil {
			line, _ := r.FieldPos(0)
			return fmt.Errorf("gtfs: stop_times.txt:%d: stop_sequence %q: %w", line, field(rec, colSeq), err)
		}

		arr, err := parseOptionalTime(field(rec, colArr))
		if err != nil {
			line, _ := r.FieldPos(0)
			return fmt.Errorf("gtfs: stop_times.txt:%d: arrival_time %q: %w", line, field(rec, colArr), err)
		}
		dep, err := parseOptionalTime(field(rec, colDep))
		if err != nil {
			line, _ := r.FieldPos(0)
			return fmt.Errorf("gtfs: stop_times.txt:%d: departure_time %q: %w", line, field(rec, colDep), err)
		}

		if arr == models.NoTime && dep == models.NoTime {
			feed.Stats.StopTimesBlankTimes++
		} else if arr == models.NoTime {
			arr = dep
		} else if dep == models.NoTime {
			dep = arr
		}
		if dep < arr {
			dep = arr
			feed.Stats.StopTimesDepBeforeArr++
		}

		var pickup, drop uint8
		if colPick >= 0 {
			pickup, err = parseEnum(field(rec, colPick), 3, 0)
			if err != nil {
				line, _ := r.FieldPos(0)
				return fmt.Errorf("gtfs: stop_times.txt:%d: pickup_type %q: %w", line, field(rec, colPick), err)
			}
		}
		if colDrop >= 0 {
			drop, err = parseEnum(field(rec, colDrop), 3, 0)
			if err != nil {
				line, _ := r.FieldPos(0)
				return fmt.Errorf("gtfs: stop_times.txt:%d: drop_off_type %q: %w", line, field(rec, colDrop), err)
			}
		}

		feed.STTrip = append(feed.STTrip, tripIdx)
		feed.STStop = append(feed.STStop, stopIdx)
		feed.STSeq = append(feed.STSeq, seq)
		feed.STArr = append(feed.STArr, arr)
		feed.STDep = append(feed.STDep, dep)
		feed.STPickup = append(feed.STPickup, pickup)
		feed.STDrop = append(feed.STDrop, drop)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Interning
// ---------------------------------------------------------------------------

// internService looks up or appends a service ID. The map key and ServiceIDs
// entry are always strings.Clone'd; lookups use the raw field (no alloc).
func internService(feed *models.Feed, raw string) (uint32, error) {
	if raw == "" {
		return 0, fmt.Errorf("empty service_id")
	}
	if idx, ok := feed.Index.Services[raw]; ok {
		return idx, nil
	}
	id := strings.Clone(raw)
	idx := uint32(len(feed.ServiceIDs))
	feed.ServiceIDs = append(feed.ServiceIDs, id)
	feed.Index.Services[id] = idx
	return idx, nil
}

// ---------------------------------------------------------------------------
// Field helpers
// ---------------------------------------------------------------------------

// columns maps header names to indices. required names must be present;
// optional names yield opt[i] == -1 when absent. Header cells are trimmed
// (BOM + surrounding space) before matching.
func columns(header []string, file string, required, optional []string) (req, opt []int, err error) {
	index := make(map[string]int, len(header))
	for i, h := range header {
		index[strings.TrimSpace(trimBOM(h))] = i
	}
	req = make([]int, len(required))
	for i, name := range required {
		idx, ok := index[name]
		if !ok {
			return nil, nil, fmt.Errorf("gtfs: %s: missing column %q", file, name)
		}
		req[i] = idx
	}
	opt = make([]int, len(optional))
	for i, name := range optional {
		if idx, ok := index[name]; ok {
			opt[i] = idx
		} else {
			opt[i] = -1
		}
	}
	return req, opt, nil
}

// field returns rec[i], or "" when i is absent / out of range.
func field(rec []string, i int) string {
	if i < 0 || i >= len(rec) {
		return ""
	}
	return rec[i]
}

// parseOptionalTime returns NoTime for a blank (after TrimSpace) field;
// otherwise delegates to parseGTFSTime (which does not re-trim).
func parseOptionalTime(s string) (int32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return models.NoTime, nil
	}
	return parseGTFSTime(s)
}

// parseGTFSTime converts a GTFS time string ("HH:MM:SS") to int32 seconds
// since service-day start. Hours 0–47 are accepted (e.g. "25:30:00" = 91800);
// never apply modulo 86400. Leading/trailing spaces are allowed.
//
// Why not time.Parse?
//  1. Performance: no allocation / locale work on millions of stop_times rows.
//  2. Correctness: time.Parse rejects hours ≥ 24; GTFS requires them.
func parseGTFSTime(s string) (int32, error) {
	// Callers on the hot path (parseOptionalTime) already TrimSpace; trim here
	// so direct callers still accept padded values.
	if len(s) > 0 && (s[0] == ' ' || s[len(s)-1] == ' ') {
		s = strings.TrimSpace(s)
	}

	// Fast path: exactly "HH:MM:SS" (dominant form in TfNSW stop_times).
	if len(s) == 8 && s[2] == ':' && s[5] == ':' {
		h0, h1 := s[0]-'0', s[1]-'0'
		m0, m1 := s[3]-'0', s[4]-'0'
		s0, s1 := s[6]-'0', s[7]-'0'
		if h0 > 9 || h1 > 9 || m0 > 9 || m1 > 9 || s0 > 9 || s1 > 9 {
			return 0, fmt.Errorf("non-digit in %q", s)
		}
		hours := int(h0)*10 + int(h1)
		minutes := int(m0)*10 + int(m1)
		seconds := int(s0)*10 + int(s1)
		if hours > 47 || minutes > 59 || seconds > 59 {
			return 0, fmt.Errorf("out-of-range time components in %q", s)
		}
		return int32(hours*3600 + minutes*60 + seconds), nil
	}

	// Minimum valid length is "0:00:00" (7 chars); also handles "H:MM:SS".
	if len(s) < 7 {
		return 0, fmt.Errorf("too short: %q", s)
	}

	lastColon := len(s) - 3 // colon before SS
	if s[lastColon] != ':' {
		return 0, fmt.Errorf("missing second colon in %q", s)
	}
	midColon := lastColon - 3 // colon before MM
	if midColon < 0 || s[midColon] != ':' {
		return 0, fmt.Errorf("missing first colon in %q", s)
	}

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

	if hours > 47 || minutes > 59 || seconds > 59 {
		return 0, fmt.Errorf("out-of-range time components in %q", s)
	}

	return int32(hours*3600 + minutes*60 + seconds), nil
}

// parseDate parses a strict yyyymmdd GTFS date into an int32, validating that
// the calendar day exists (rejects 20240230, etc.).
func parseDate(s string) (int32, error) {
	s = strings.TrimSpace(s)
	if len(s) != 8 {
		return 0, fmt.Errorf("want yyyymmdd, got %q", s)
	}
	y, err := atoiBytes(s[0:4])
	if err != nil {
		return 0, fmt.Errorf("invalid year in %q: %w", s, err)
	}
	m, err := atoiBytes(s[4:6])
	if err != nil {
		return 0, fmt.Errorf("invalid month in %q: %w", s, err)
	}
	d, err := atoiBytes(s[6:8])
	if err != nil {
		return 0, fmt.Errorf("invalid day in %q: %w", s, err)
	}
	t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	if t.Year() != y || int(t.Month()) != m || t.Day() != d {
		return 0, fmt.Errorf("invalid calendar date %q", s)
	}
	return int32(y*10000 + m*100 + d), nil
}

// parseEnum parses a small non-negative integer field. Blank => def.
// Values greater than max are an error.
func parseEnum(s string, max, def uint8) (uint8, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return def, nil
	}
	n, err := atoiBytes(s)
	if err != nil {
		return 0, err
	}
	if n < 0 || n > int(max) {
		return 0, fmt.Errorf("value %d out of range 0..%d", n, max)
	}
	return uint8(n), nil
}

// parseUint32 parses an unsigned 32-bit integer via atoiBytes with overflow check.
func parseUint32(s string) (uint32, error) {
	s = strings.TrimSpace(s)
	n, err := atoiBytes(s)
	if err != nil {
		return 0, err
	}
	if n < 0 || n > math.MaxUint32 {
		return 0, fmt.Errorf("uint32 overflow: %d", n)
	}
	return uint32(n), nil
}

// parseDayFlag requires exactly "0" or "1" (after trim).
func parseDayFlag(s string) (uint8, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "0":
		return 0, nil
	case "1":
		return 1, nil
	default:
		return 0, fmt.Errorf("want 0 or 1, got %q", s)
	}
}

// atoiBytes converts a small ASCII digit string to an int without allocating.
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

// trimBOM strips the UTF-8 byte-order mark that some GTFS producers prepend to
// the first field of the first line (a common Excel/Windows artifact).
func trimBOM(s string) string {
	if len(s) >= 3 && s[0] == 0xEF && s[1] == 0xBB && s[2] == 0xBF {
		return s[3:]
	}
	return s
}
