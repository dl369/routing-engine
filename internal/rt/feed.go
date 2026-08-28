// Package rt holds unmarshaled GTFS-RT entities and an in-process cache loaded
// from Redis on a fixed interval.
package rt

import (
	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

// EntityKind identifies which GTFS-RT payload an Entity carries.
type EntityKind string

const (
	KindTripUpdate EntityKind = "trip_update"
	KindVehicle    EntityKind = "vehicle"
	KindAlert      EntityKind = "alert"
)

// Entity is a flat view of one FeedEntity for routing overlays.
type Entity struct {
	ID       string
	Kind     EntityKind
	TripID   string
	RouteID  string
	DelaySec int32
	StopID   string
	Lat      float64
	Lon      float64
	Bearing  float32
	Speed    float32
	Cause    string
	Effect   string
	Header   string
}

// FromFeedMessage converts a protobuf FeedMessage into a new entity slice.
func FromFeedMessage(msg *gtfsrt.FeedMessage) []Entity {
	entities := msg.GetEntity()
	out := make([]Entity, 0, len(entities))

	for _, e := range entities {
		ent := Entity{ID: e.GetId()}

		switch {
		case e.TripUpdate != nil:
			ent.Kind = KindTripUpdate
			tu := e.GetTripUpdate()
			trip := tu.GetTrip()
			ent.TripID = trip.GetTripId()
			ent.RouteID = trip.GetRouteId()

			if stu := tu.GetStopTimeUpdate(); len(stu) > 0 {
				first := stu[0]
				ent.StopID = first.GetStopId()
				if dep := first.GetDeparture(); dep != nil {
					ent.DelaySec = dep.GetDelay()
				} else if arr := first.GetArrival(); arr != nil {
					ent.DelaySec = arr.GetDelay()
				}
			}

		case e.Vehicle != nil:
			ent.Kind = KindVehicle
			vp := e.GetVehicle()
			trip := vp.GetTrip()
			ent.TripID = trip.GetTripId()
			ent.RouteID = trip.GetRouteId()
			if pos := vp.GetPosition(); pos != nil {
				ent.Lat = float64(pos.GetLatitude())
				ent.Lon = float64(pos.GetLongitude())
				ent.Bearing = pos.GetBearing()
				ent.Speed = pos.GetSpeed()
			}

		case e.Alert != nil:
			ent.Kind = KindAlert
			al := e.GetAlert()
			ent.Cause = al.GetCause().String()
			ent.Effect = al.GetEffect().String()
			if tt := al.GetHeaderText(); tt != nil {
				if tr := tt.GetTranslation(); len(tr) > 0 {
					ent.Header = tr[0].GetText()
				}
			}

		default:
			continue
		}

		out = append(out, ent)
	}

	return out
}
