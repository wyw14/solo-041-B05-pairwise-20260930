package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type DeviceKind string

const (
	DeviceBlower DeviceKind = "blower"
	DevicePump   DeviceKind = "pump"
	DeviceValve  DeviceKind = "valve"
	DeviceDoser  DeviceKind = "doser"
)

// ErrStaleDeviceCommand is returned when a command arrives after a newer
// command for the same device has already been applied. Callers must treat it
// as a benign skip, not a failure worth driving the device again.
var ErrStaleDeviceCommand = errors.New("device command is stale")

type DeviceCommand struct {
	ID         uuid.UUID  `json:"id"`
	LineID     LineID     `json:"line_id"`
	BatchID    uuid.UUID  `json:"batch_id"`
	DeviceID   string     `json:"device_id"`
	Kind       DeviceKind `json:"kind"`
	Generation uint64     `json:"generation"`
	// Revision orders commands issued within the same generation. It mirrors
	// the journal sequence of the originating event, so the order committed to
	// the event log is the order observed on the device bus.
	Revision uint64            `json:"revision"`
	Action   string            `json:"action"`
	Setpoint float64           `json:"setpoint,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	IssuedAt time.Time         `json:"issued_at"`
}

func NewDeviceCommand(lineID LineID, batchID uuid.UUID, deviceID string, kind DeviceKind, generation, revision uint64, action string, now time.Time) (DeviceCommand, error) {
	if lineID == "" || batchID == uuid.Nil || deviceID == "" || generation == 0 || revision == 0 || action == "" {
		return DeviceCommand{}, errors.New("device command identity is incomplete")
	}
	return DeviceCommand{
		ID: uuid.New(), LineID: lineID, BatchID: batchID, DeviceID: deviceID,
		Kind: kind, Generation: generation, Revision: revision, Action: action, IssuedAt: now.UTC(),
		Metadata: make(map[string]string),
	}, nil
}

// NewerThan reports whether c strictly supersedes other for the same device.
// Commands order by generation, then by intra-generation revision; IssuedAt is
// the final tie-breaker so distinct commands sharing a key still order
// deterministically. Equal commands are never considered "newer".
func (c DeviceCommand) NewerThan(other DeviceCommand) bool {
	if c.Generation != other.Generation {
		return c.Generation > other.Generation
	}
	if c.Revision != other.Revision {
		return c.Revision > other.Revision
	}
	if !c.IssuedAt.Equal(other.IssuedAt) {
		return c.IssuedAt.After(other.IssuedAt)
	}
	return false
}
