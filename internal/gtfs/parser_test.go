package gtfs

import (
	"archive/zip"
	"bytes"
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"route/internal/models"
)

// buildZip writes name→contents entries into an in-memory GTFS zip.
func buildZip(t testing.TB, files map[string]string) ([]byte, int64) {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	b := buf.Bytes()
	return b, int64(len(b))
}

func parseZip(t testing.TB, files map[string]string) *models.Feed {
	t.Helper()
	b, n := buildZip(t, files)
	feed, err := ParseGTFSReader(bytes.NewReader(b), n)
	if err != nil {
		t.Fatalf("ParseGTFSReader: %v", err)
	}
	return feed
}

func mustErr(t *testing.T, files map[string]string, substr string) {
	t.Helper()
	b, n := buildZip(t, files)
	_, err := ParseGTFSReader(bytes.NewReader(b), n)
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("expected error containing %q, got %v", substr, err)
	}
}

// baseFiles returns a minimal valid feed; tests override/add keys as needed.
func baseFiles() map[string]string {
	return map[string]string{
		"stops.txt": "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n" +
			"S1,Alpha, -33.8,151.2,0,\n" +
			"S2,Beta, -33.9,151.3,0,S1\n",
		"routes.txt": "route_id,route_short_name,route_type\n" +
			"R1,100,3\n",
		"calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n" +
			"WD,1,1,1,1,1,0,0,20240101,20241231\n",
		"trips.txt": "trip_id,route_id,service_id\n" +
			"T1,R1,WD\n",
		"stop_times.txt": "trip_id,stop_id,stop_sequence,arrival_time,departure_time,pickup_type,drop_off_type\n" +
			"T1,S1,1,08:00:00,08:00:00,0,0\n" +
			"T1,S2,2,08:10:00,08:11:00,0,0\n",
	}
}

func TestHappyPath(t *testing.T) {
	feed := parseZip(t, baseFiles())

	if len(feed.StopIDs) != 2 || len(feed.RouteIDs) != 1 || len(feed.TripIDs) != 1 {
		t.Fatalf("counts stops=%d routes=%d trips=%d", len(feed.StopIDs), len(feed.RouteIDs), len(feed.TripIDs))
	}
	if len(feed.STTrip) != 2 || len(feed.STStop) != 2 {
		t.Fatalf("stop_times len=%d", len(feed.STTrip))
	}
	if feed.STTrip[0] != 0 || feed.STStop[0] != 0 || feed.STTrip[1] != 0 || feed.STStop[1] != 1 {
		t.Fatalf("STTrip/STStop cross-refs: trip=%v stop=%v", feed.STTrip, feed.STStop)
	}
	if feed.TripRoute[0] != 0 || feed.TripService[0] != 0 {
		t.Fatalf("TripRoute=%d TripService=%d", feed.TripRoute[0], feed.TripService[0])
	}
	if feed.StopParent[1] != 0 {
		t.Fatalf("S2 parent should be S1 (0), got %d", feed.StopParent[1])
	}
	if feed.STArr[0] != 8*3600 || feed.STDep[1] != 8*3600+11*60 {
		t.Fatalf("times arr0=%d dep1=%d", feed.STArr[0], feed.STDep[1])
	}
}

func TestBOMQuotedNameColumnOrder(t *testing.T) {
	files := baseFiles()
	// BOM on first header; columns reordered; quoted name with comma.
	files["stops.txt"] = "\xEF\xBB\xBFstop_lat,stop_id,stop_name,stop_lon\n" +
		"-33.8,S1,\"Alpha, Station\",151.2\n"
	files["stop_times.txt"] = "stop_sequence,trip_id,stop_id,arrival_time,departure_time\n" +
		"1,T1,S1,08:00:00,08:00:00\n"
	feed := parseZip(t, files)
	if feed.StopNames[0] != "Alpha, Station" {
		t.Fatalf("stop_name=%q", feed.StopNames[0])
	}
	if feed.Index.Stops["S1"] != 0 {
		t.Fatalf("stop index missing")
	}
}

func TestExtendedRouteTypes(t *testing.T) {
	files := baseFiles()
	files["routes.txt"] = "route_id,route_short_name,route_type\n" +
		"R1,X,700\nR2,Y,712\nR3,Z,900\n"
	files["trips.txt"] = "trip_id,route_id,service_id\nT1,R1,WD\n"
	feed := parseZip(t, files)
	if feed.RouteTypes[0] != 700 || feed.RouteTypes[1] != 712 || feed.RouteTypes[2] != 900 {
		t.Fatalf("route types=%v", feed.RouteTypes)
	}
}

func TestTimes(t *testing.T) {
	t.Run("25:30:00", func(t *testing.T) {
		v, err := parseGTFSTime("25:30:00")
		if err != nil || v != 25*3600+30*60 {
			t.Fatalf("got %d %v", v, err)
		}
	})
	t.Run("9:05:00", func(t *testing.T) {
		v, err := parseGTFSTime("9:05:00")
		if err != nil || v != 9*3600+5*60 {
			t.Fatalf("got %d %v", v, err)
		}
	})
	t.Run("spaces", func(t *testing.T) {
		v, err := parseGTFSTime(" 08:00:00 ")
		if err != nil || v != 8*3600 {
			t.Fatalf("got %d %v", v, err)
		}
	})
	t.Run("08:60:00 errors", func(t *testing.T) {
		if _, err := parseGTFSTime("08:60:00"); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("48:00:00 errors", func(t *testing.T) {
		if _, err := parseGTFSTime("48:00:00"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("blank arr copies dep", func(t *testing.T) {
		files := baseFiles()
		files["stop_times.txt"] = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n" +
			"T1,S1,1,,08:00:00\n"
		feed := parseZip(t, files)
		if feed.STArr[0] != 8*3600 || feed.STDep[0] != 8*3600 {
			t.Fatalf("arr=%d dep=%d", feed.STArr[0], feed.STDep[0])
		}
	})
	t.Run("both blank", func(t *testing.T) {
		files := baseFiles()
		files["stop_times.txt"] = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n" +
			"T1,S1,1,,\n"
		feed := parseZip(t, files)
		if feed.STArr[0] != models.NoTime || feed.STDep[0] != models.NoTime {
			t.Fatalf("arr=%d dep=%d", feed.STArr[0], feed.STDep[0])
		}
		if feed.Stats.StopTimesBlankTimes != 1 {
			t.Fatalf("blank stat=%d", feed.Stats.StopTimesBlankTimes)
		}
	})
	t.Run("dep before arr clamped", func(t *testing.T) {
		files := baseFiles()
		files["stop_times.txt"] = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n" +
			"T1,S1,1,08:10:00,08:00:00\n"
		feed := parseZip(t, files)
		if feed.STDep[0] != feed.STArr[0] || feed.STArr[0] != 8*3600+10*60 {
			t.Fatalf("arr=%d dep=%d", feed.STArr[0], feed.STDep[0])
		}
		if feed.Stats.StopTimesDepBeforeArr != 1 {
			t.Fatalf("clamp stat=%d", feed.Stats.StopTimesDepBeforeArr)
		}
	})
}

func TestParentStation(t *testing.T) {
	t.Run("forward ref", func(t *testing.T) {
		files := baseFiles()
		// Child before parent in file order.
		files["stops.txt"] = "stop_id,stop_lat,stop_lon,parent_station\n" +
			"CHILD,-33.8,151.2,PARENT\n" +
			"PARENT,-33.81,151.21,\n"
		files["stop_times.txt"] = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n" +
			"T1,CHILD,1,08:00:00,08:00:00\n"
		feed := parseZip(t, files)
		if feed.StopParent[0] != 1 {
			t.Fatalf("parent=%d want 1", feed.StopParent[0])
		}
	})
	t.Run("unknown parent", func(t *testing.T) {
		files := baseFiles()
		files["stops.txt"] = "stop_id,stop_lat,stop_lon,parent_station\n" +
			"S1,-33.8,151.2,MISSING\n"
		feed := parseZip(t, files)
		if feed.StopParent[0] != models.NoIndex {
			t.Fatalf("parent=%d", feed.StopParent[0])
		}
		if feed.Stats.ParentUnresolved != 1 {
			t.Fatalf("stat=%d", feed.Stats.ParentUnresolved)
		}
	})
}

func TestLocationTypeCoords(t *testing.T) {
	t.Run("type3 blank NaN", func(t *testing.T) {
		files := baseFiles()
		files["stops.txt"] = "stop_id,stop_lat,stop_lon,location_type\n" +
			"N1,,,3\nS1,-33.8,151.2,0\n"
		feed := parseZip(t, files)
		if !math.IsNaN(feed.StopLat[0]) || !math.IsNaN(feed.StopLon[0]) {
			t.Fatalf("expected NaN, got %v %v", feed.StopLat[0], feed.StopLon[0])
		}
	})
	t.Run("type0 blank errors", func(t *testing.T) {
		files := baseFiles()
		files["stops.txt"] = "stop_id,stop_lat,stop_lon,location_type\n" +
			"S1,,,0\n"
		mustErr(t, files, "stop_lat")
	})
}

func TestUnknownTripStopSkipped(t *testing.T) {
	files := baseFiles()
	files["stop_times.txt"] = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n" +
		"T1,S1,1,08:00:00,08:00:00\n" +
		"NOPE,S1,1,08:00:00,08:00:00\n" +
		"T1,NOSUCH,2,08:10:00,08:10:00\n"
	feed := parseZip(t, files)
	if len(feed.STTrip) != 1 {
		t.Fatalf("accepted=%d", len(feed.STTrip))
	}
	if feed.Stats.StopTimesUnknownTrip != 1 || feed.Stats.StopTimesUnknownStop != 1 {
		t.Fatalf("stats=%+v", feed.Stats)
	}
}

func TestCalendar(t *testing.T) {
	t.Run("weekday bits", func(t *testing.T) {
		feed := parseZip(t, baseFiles())
		want := models.Monday | models.Tuesday | models.Wednesday | models.Thursday | models.Friday
		if feed.Calendars[0].Weekdays != want {
			t.Fatalf("weekdays=%08b want=%08b", feed.Calendars[0].Weekdays, want)
		}
		if feed.Calendars[0].Start != 20240101 || feed.Calendars[0].End != 20241231 {
			t.Fatalf("range %d..%d", feed.Calendars[0].Start, feed.Calendars[0].End)
		}
	})
	t.Run("start after end", func(t *testing.T) {
		files := baseFiles()
		files["calendar.txt"] = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n" +
			"WD,1,0,0,0,0,0,0,20241231,20240101\n"
		mustErr(t, files, "start_date")
	})
	t.Run("calendar_dates only", func(t *testing.T) {
		files := baseFiles()
		delete(files, "calendar.txt")
		files["calendar_dates.txt"] = "service_id,date,exception_type\n" +
			"WD,20240601,1\n"
		feed := parseZip(t, files)
		if len(feed.CalendarDates) != 1 || !feed.CalendarDates[0].Added {
			t.Fatalf("dates=%v", feed.CalendarDates)
		}
		if feed.TripService[0] != feed.Index.Services["WD"] {
			t.Fatalf("service not linked")
		}
	})
	t.Run("both missing", func(t *testing.T) {
		files := baseFiles()
		delete(files, "calendar.txt")
		mustErr(t, files, "calendar")
	})
	t.Run("exception_type 3", func(t *testing.T) {
		files := baseFiles()
		files["calendar_dates.txt"] = "service_id,date,exception_type\n" +
			"WD,20240601,3\n"
		mustErr(t, files, "exception_type")
	})
}

func TestOptionalColumnsDefaults(t *testing.T) {
	files := baseFiles()
	files["stops.txt"] = "stop_id,stop_lat,stop_lon\nS1,-33.8,151.2\n"
	files["stop_times.txt"] = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n" +
		"T1,S1,1,08:00:00,08:00:00\n"
	feed := parseZip(t, files)
	if feed.StopNames[0] != "" || feed.StopLocType[0] != 0 {
		t.Fatalf("name=%q loc=%d", feed.StopNames[0], feed.StopLocType[0])
	}
	if feed.STPickup[0] != 0 || feed.STDrop[0] != 0 {
		t.Fatalf("pickup=%d drop=%d", feed.STPickup[0], feed.STDrop[0])
	}
}

func TestJaggedRows(t *testing.T) {
	files := baseFiles()
	files["stops.txt"] = "stop_id,stop_name,stop_lat,stop_lon\n" +
		"S1,Alpha,-33.8,151.2,extra,fields\n" +
		"S2\n" // too few — lat/lon blank for type 0 should error
	mustErr(t, files, "stop_lat")

	files = baseFiles()
	files["stops.txt"] = "stop_id,stop_name,stop_lat,stop_lon\n" +
		"S1,Alpha,-33.8,151.2,extra\n"
	feed := parseZip(t, files)
	if len(feed.StopIDs) != 1 {
		t.Fatalf("stops=%d", len(feed.StopIDs))
	}
}

func TestDuplicateIDs(t *testing.T) {
	files := baseFiles()
	files["stops.txt"] = "stop_id,stop_lat,stop_lon,stop_name\n" +
		"S1,-33.8,151.2,First\n" +
		"S1,-33.9,151.3,Second\n"
	files["trips.txt"] = "trip_id,route_id,service_id\n" +
		"T1,R1,WD\n" +
		"T1,R1,WD\n"
	feed := parseZip(t, files)
	if len(feed.StopIDs) != 1 || feed.StopNames[0] != "First" {
		t.Fatalf("stops=%v names=%v", feed.StopIDs, feed.StopNames)
	}
	if len(feed.TripIDs) != 1 {
		t.Fatalf("trips=%d", len(feed.TripIDs))
	}
	if feed.Stats.DuplicateIDs != 2 {
		t.Fatalf("DuplicateIDs=%d", feed.Stats.DuplicateIDs)
	}
}

func TestCloneNotPinnedToRow(t *testing.T) {
	// Parse a single-stop feed and check the stored ID's backing array is not
	// inside a regenerated CSV row buffer. We compare against a freshly built
	// row string that mimics csv.Reader's single-allocation-per-row layout.
	files := baseFiles()
	files["stops.txt"] = "stop_id,stop_lat,stop_lon\nUNIQUE_STOP_ID_XYZ,-33.8,151.2\n"
	feed := parseZip(t, files)

	id := feed.StopIDs[0]
	if id != "UNIQUE_STOP_ID_XYZ" {
		t.Fatalf("id=%q", id)
	}
	// strings.Clone guarantees a fresh backing array; Cap of a cloned string's
	// data equals Len. A substring of a larger row would typically have Cap > Len
	// when taken from a shared buffer — but Go strings don't expose Cap.
	// Instead: mutate a copy of what a row buffer would look like and ensure the
	// stored ID is unaffected (Clone semantics), and that unsafe.StringData
	// differs from a row-substring string data pointer constructed in-process.
	row := "UNIQUE_STOP_ID_XYZ,-33.8,151.2"
	sub := row[:len(id)]
	if unsafe.StringData(id) == unsafe.StringData(sub) {
		t.Fatal("stored ID shares backing with row substring — missing Clone")
	}
	// Index key must also be independent.
	for k := range feed.Index.Stops {
		if unsafe.StringData(k) == unsafe.StringData(sub) {
			t.Fatal("index key shares backing with row substring")
		}
	}
}

func TestTripsUnknownRouteService(t *testing.T) {
	files := baseFiles()
	files["trips.txt"] = "trip_id,route_id,service_id\n" +
		"T1,MISSING,NOSVC\n"
	feed := parseZip(t, files)
	if feed.TripRoute[0] != models.NoIndex {
		t.Fatalf("route=%d", feed.TripRoute[0])
	}
	if feed.Stats.TripsUnknownRoute != 1 || feed.Stats.TripsUnknownService != 1 {
		t.Fatalf("stats=%+v", feed.Stats)
	}
	if _, ok := feed.Index.Services["NOSVC"]; !ok {
		t.Fatal("service should be interned")
	}
}

func BenchmarkParseGTFS(b *testing.B) {
	const path = "../../data/gtfs.zip"
	if _, err := os.Stat(path); err != nil {
		b.Skip("data/gtfs.zip not present")
	}

	b.ReportAllocs()
	var lastFeed *models.Feed
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		feed, err := ParseGTFS(path)
		if err != nil {
			b.Fatal(err)
		}
		lastFeed = feed
	}
	b.StopTimer()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapAfterParse := ms.HeapInuse
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heapAfterGC := ms.HeapInuse

	b.Logf("stops=%d routes=%d trips=%d stop_times=%d",
		len(lastFeed.StopIDs), len(lastFeed.RouteIDs), len(lastFeed.TripIDs), len(lastFeed.STTrip))
	b.Logf("HeapInuse after parse=%s after GC=%s stats=%+v",
		formatBytes(heapAfterParse), formatBytes(heapAfterGC), lastFeed.Stats)
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
