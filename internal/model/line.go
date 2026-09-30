package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type LineID string

func ParseLineID(raw string) (LineID, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("process line is required")
	}
	if len(value) > 32 {
		return "", errors.New("process line is too long")
	}
	return LineID(value), nil
}

type ProcessStage string

const (
	StageReceiving   ProcessStage = "receiving"
	StageEqualizing  ProcessStage = "equalizing"
	StageBiological  ProcessStage = "biological"
	StageSettling    ProcessStage = "settling"
	StageQualified   ProcessStage = "qualified"
	StageDischarging ProcessStage = "discharging"
	StageClosed      ProcessStage = "closed"
	StageIsolated    ProcessStage = "isolated"
)

var stageOrder = map[ProcessStage]int{
	StageReceiving: 1, StageEqualizing: 2, StageBiological: 3,
	StageSettling: 4, StageQualified: 5, StageDischarging: 6,
	StageClosed: 7,
}

type LineState struct {
	ID           LineID       `json:"id"`
	Stage        ProcessStage `json:"stage"`
	BatchID      string       `json:"batch_id,omitempty"`
	Generation   uint64       `json:"generation"`
	Emergency    bool         `json:"emergency"`
	UpdatedAt    time.Time    `json:"updated_at"`
	LastEventSeq uint64       `json:"last_event_seq"`
}

func NewLineState(id LineID, now time.Time) LineState {
	return LineState{ID: id, Stage: StageReceiving, Generation: 1, UpdatedAt: now.UTC()}
}

func (s LineState) CanAdvance(next ProcessStage) bool {
	if s.Emergency || s.Stage == StageIsolated || s.Stage == StageClosed {
		return false
	}
	return stageOrder[next] == stageOrder[s.Stage]+1
}

func (s LineState) Advance(next ProcessStage, now time.Time) (LineState, error) {
	if !s.CanAdvance(next) {
		return s, fmt.Errorf("cannot move line %s from %s to %s", s.ID, s.Stage, next)
	}
	s.Stage = next
	s.UpdatedAt = now.UTC()
	return s, nil
}

func (s LineState) Isolate(sequence uint64, now time.Time) LineState {
	s.Stage = StageIsolated
	s.Emergency = true
	s.Generation++
	s.LastEventSeq = sequence
	s.UpdatedAt = now.UTC()
	return s
}

// IsolateAt moves the line into emergency at a specific, already-decided
// generation. Unlike Isolate it does not bump the generation, so replaying an
// emergency event during recovery converges on the same generation instead of
// advancing it on every restart.
func (s LineState) IsolateAt(generation, sequence uint64, now time.Time) LineState {
	if generation > s.Generation {
		s.Generation = generation
	}
	s.Stage = StageIsolated
	s.Emergency = true
	s.LastEventSeq = sequence
	s.UpdatedAt = now.UTC()
	return s
}

func (s LineState) Clone() LineState {
	return s
}
