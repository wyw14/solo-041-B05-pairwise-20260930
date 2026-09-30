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

// ErrStaleDeviceCommand is returned when a command belongs to an older
// generation, or to an earlier revision within the same generation, than the
// command already accepted for its device.
var ErrStaleDeviceCommand = errors.New("device command is stale")

// ErrDeviceCommandConflict is returned when two distinct commands claim the
// same (generation, revision) position for one device.
var ErrDeviceCommandConflict = errors.New("device command revision conflicts with another command")

type DeviceCommand struct {
	ID         uuid.UUID         `json:"id"`
	LineID     LineID            `json:"line_id"`
	BatchID    uuid.UUID         `json:"batch_id"`
	DeviceID   string            `json:"device_id"`
	Kind       DeviceKind        `json:"kind"`
	Generation uint64            `json:"generation"`
	Revision   uint64            `json:"revision,omitempty"`
	Action     string            `json:"action"`
	Setpoint   float64           `json:"setpoint,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	IssuedAt   time.Time         `json:"issued_at"`
}

func NewDeviceCommand(lineID LineID, batchID uuid.UUID, deviceID string, kind DeviceKind, generation uint64, action string, now time.Time) (DeviceCommand, error) {
	if lineID == "" || batchID == uuid.Nil || deviceID == "" || generation == 0 || action == "" {
		return DeviceCommand{}, errors.New("device command identity is incomplete")
	}
	return DeviceCommand{
		ID: uuid.New(), LineID: lineID, BatchID: batchID, DeviceID: deviceID,
		Kind: kind, Generation: generation, Action: action, IssuedAt: now.UTC(),
		Metadata: make(map[string]string),
	}, nil
}
