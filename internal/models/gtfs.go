// Package models defines the core in-memory data structures for the Route
// transit engine. The static parser emits a pointer-free columnar Feed; the
// future router.Build step turns that into hot-path routing structures.
package models

import "math"

const (
	NoIndex uint32 = math.MaxUint32 // unresolved / absent dense reference
	NoTime  int32  = math.MinInt32  // blank arrival/departure (non-timepoint)
)

// Weekday bits for Calendar.Weekdays: bit0 = Monday … bit6 = Sunday.
const (
	Monday uint8 = 1 << iota
	Tuesday
	Wednesday
	Thursday
	Friday
	Saturday
	Sunday
)

// Calendar is one calendar.txt row: a service active on selected weekdays
// between Start and End (inclusive), expressed as yyyymmdd integers.
type Calendar struct {
	Service  uint32 // index into Feed.ServiceIDs
	Weekdays uint8
	Start    int32 // yyyymmdd, inclusive
	End      int32 // yyyymmdd, inclusive
}

// CalendarDate is one calendar_dates.txt exception for a service.
type CalendarDate struct {
	Service uint32
	Date    int32 // yyyymmdd
	Added   bool  // exception_type 1 = added, 2 = removed
}

// FeedIndex maps GTFS string IDs to dense indices. Cold: used by router.Build
// and RT trip matching, never by the routing hot path. Keys are strings.Clone'd.
type FeedIndex struct {
	Stops, Routes, Trips, Services map[string]uint32
}

// FeedStats counts tolerated data-quality issues for the startup log.
type FeedStats struct {
	StopTimesUnknownTrip, StopTimesUnknownStop uint32
	StopTimesBlankTimes                        uint32 // rows with NoTime in arr or dep
	StopTimesDepBeforeArr                      uint32 // clamped dep = arr
	TripsUnknownRoute, TripsUnknownService     uint32
	DuplicateIDs                               uint32
	ParentUnresolved                           uint32
}

// Feed is the pointer-free columnar staging form of a GTFS static feed.
// Row i of each per-entity column group describes entity with dense index i.
//
// Feed is consumed once by router.Build and then discarded — it is NOT the
// routing hot-path representation. Columns use value types only (no pointers
// inside numeric slices) so the GC has almost nothing to scan after parse.
type Feed struct {
	// Stops (dense index = first-seen row order in stops.txt)
	StopIDs, StopNames []string
	StopLat, StopLon   []float64 // NaN allowed only for location_type 3/4
	StopLocType        []uint8   // 0 stop/platform,1 station,2 entrance,3 node,4 boarding area
	StopParent         []uint32  // NoIndex if none or unresolved

	// Routes
	RouteIDs, RouteShortNames []string
	RouteTypes                []uint16 // uint16: TfNSW uses extended types (700, 712, 401, 900…)

	// Services
	ServiceIDs    []string
	Calendars     []Calendar
	CalendarDates []CalendarDate

	// Trips
	TripIDs     []string
	TripRoute   []uint32 // NoIndex if route unknown
	TripService []uint32 // services first seen in trips.txt are interned (no calendar => never active)

	// Stop times: parallel columns, one entry per accepted row, file order (NOT sorted)
	STTrip   []uint32
	STStop   []uint32
	STSeq    []uint32
	STArr    []int32 // NoTime if blank
	STDep    []int32 // NoTime if blank
	STPickup []uint8 // 0 regular,1 none,2 phone agency,3 coordinate with driver
	STDrop   []uint8

	Index FeedIndex
	Stats FeedStats
}
