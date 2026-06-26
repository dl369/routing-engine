// Package models defines the core in-memory data structures for the Route
// transit engine. Every struct is designed for cache locality: flat fields,
// no pointer indirection inside hot-path types, and int32 times so four
// timestamps fit in a single 64-byte cache line alongside other fields.
package models

// Stop represents a physical boarding/alighting node in the network.
type Stop struct {
	ID  string
	Lat float64
	Lon float64
}

// Route represents a distinct named transit line (bus route, rail line, etc.).
type Route struct {
	ID        string
	ShortName string
	// Type follows the GTFS route_type specification:
	//   0 = Tram/Light Rail, 1 = Subway/Metro, 2 = Rail,
	//   3 = Bus, 4 = Ferry, 5 = Cable Tram, 6 = Aerial Lift, 7 = Funicular
	Type int
}

// StopTime represents a single scheduled stop event within a trip.
//
// Arrival and departure times are stored as int32 seconds-past-midnight rather
// than time.Time values. This has three benefits:
//  1. Size: 4 bytes vs 24 bytes per field — more events fit per cache line.
//  2. Math: computing travel durations is a single integer subtraction.
//  3. Correctness: GTFS allows times > 24 h (e.g. 25:30:00 for post-midnight
//     services), which time.Time cannot represent without date context.
type StopTime struct {
	StopID        string
	Sequence      int32
	ArrivalTime   int32 // seconds past midnight
	DepartureTime int32 // seconds past midnight
}

// Trip represents a single scheduled run of a route.
// StopTimes is a contiguous slice sorted ascending by Sequence so the routing
// algorithm can walk it with a tight, branch-predictable loop.
type Trip struct {
	ID        string
	RouteID   string
	StopTimes []StopTime // sorted by Sequence after parsing
}

// TransitGraph is the top-level container for the entire parsed static network.
// It is the single object handed to every routing algorithm at startup.
type TransitGraph struct {
	Stops  map[string]*Stop
	Routes map[string]*Route
	Trips  map[string]*Trip
}
