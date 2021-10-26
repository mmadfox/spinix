package spinix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrStateNotFound = errors.New("spinix/states: state not found")

type States interface {
	Lookup(ctx context.Context, k StateID) (*State, error)
	Update(ctx context.Context, s *State) error
	Make(ctx context.Context, k StateID) (*State, error)
	Remove(ctx context.Context, k StateID) error
	RemoveByRule(ctx context.Context, ruleID string) error
	RemoveByDevice(ctx context.Context, deviceID string) error
}

var _ States = &memoryState{}

type memoryState struct {
	indexByID     stateIndex
	indexByRule   stateByValueIndex
	indexByDevice stateByValueIndex
}

func NewMemoryState() *memoryState {
	return &memoryState{
		indexByID:     newStateIndex(),
		indexByRule:   newStateByValueIndex(),
		indexByDevice: newStateByValueIndex(),
	}
}

func (ms *memoryState) Lookup(_ context.Context, k StateID) (*State, error) {
	return ms.indexByID.get(k)
}

func (ms *memoryState) Update(_ context.Context, s *State) error {
	return ms.indexByID.update(s)
}

func (ms *memoryState) Make(_ context.Context, sid StateID) (*State, error) {
	if err := sid.validate(); err != nil {
		return nil, err
	}
	state := NewState(sid)
	ms.indexByID.add(state)
	ms.indexByRule.add(sid.RuleID, sid.IMEI, state)
	ms.indexByDevice.add(sid.IMEI, sid.RuleID, state)
	return state, nil
}

func (ms *memoryState) Remove(_ context.Context, k StateID) error {
	if err := ms.indexByID.remove(k); err != nil {
		return err
	}
	ms.indexByDevice.remove(k.IMEI, k.RuleID)
	ms.indexByRule.remove(k.RuleID, k.IMEI)
	return nil
}

func (ms *memoryState) RemoveByRule(_ context.Context, ruleID string) (err error) {
	err = ms.indexByRule.deleteAndIter(ruleID, func(s *State) error {
		if err := ms.indexByID.remove(s.ID()); err != nil {
			return err
		}
		ms.indexByDevice.remove(s.ID().IMEI, s.ID().RuleID)
		return nil
	})
	return
}

func (ms *memoryState) RemoveByDevice(_ context.Context, deviceID string) (err error) {
	err = ms.indexByDevice.deleteAndIter(deviceID, func(s *State) error {
		if err := ms.indexByID.remove(s.ID()); err != nil {
			return err
		}
		ms.indexByRule.remove(s.ID().RuleID, s.ID().IMEI)
		return nil
	})
	return
}

func newStateIndex() stateIndex {
	buckets := make([]*stateBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &stateBucket{
			index: make(map[StateID]*State),
		}
	}
	return buckets
}

func newStateByValueIndex() stateByValueIndex {
	buckets := make([]*stateByValueBucket, numBucket/2)
	for i := 0; i < numBucket/2; i++ {
		buckets[i] = &stateByValueBucket{
			index: make(map[string]map[string]*State),
		}
	}
	return buckets
}

type StateID struct {
	IMEI   string
	RuleID string
}

func (s StateID) String() string {
	return s.IMEI + ":" + s.RuleID
}

type State struct {
	id            StateID
	now           int64
	lastSeenTime  int64
	lastResetTime int64
	hits          int
	objectsVisits map[string]int64
}

type StateSnapshot struct {
	ID            StateID          `json:"regionFromLatLon"`
	Now           int64            `json:"now"`
	LastSeenTime  int64            `json:"lastSeenTime"`
	LastResetTime int64            `json:"lastResetTime"`
	Hits          int              `json:"hits"`
	ObjectsVisits map[string]int64 `json:"objectsVisits"`
}

func (s StateSnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StateSnapshot) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *State) FromSnapshot(snap StateSnapshot) {
	s.id = snap.ID
	s.now = snap.Now
	s.lastSeenTime = snap.LastSeenTime
	s.lastResetTime = snap.LastResetTime
	s.hits = snap.Hits
	s.objectsVisits = make(map[string]int64)
	for k, v := range snap.ObjectsVisits {
		s.objectsVisits[k] = v
	}
}

func (s *State) Snapshot() StateSnapshot {
	snapshot := StateSnapshot{
		ID:            s.id,
		Now:           s.now,
		LastSeenTime:  s.lastSeenTime,
		LastResetTime: s.lastResetTime,
		Hits:          s.hits,
		ObjectsVisits: make(map[string]int64),
	}
	for k, v := range s.objectsVisits {
		snapshot.ObjectsVisits[k] = v
	}
	return snapshot
}

func (s *State) SetTime(now int64) {
	if now <= 0 {
		return
	}
	s.now = now
}

func (s *State) Reset() {
	s.lastResetTime = 0
	s.lastSeenTime = 0
	s.hits = 0
	s.objectsVisits = make(map[string]int64)
}

func (s *State) ID() StateID {
	return s.id
}

func (s *State) NeedReset(interval time.Duration) bool {
	if interval.Seconds() == 0 {
		return true
	}
	if s.lastResetTime == 0 {
		return true
	}
	if s.now == 0 {
		s.now = time.Now().Unix()
	}
	diff := s.now - s.lastResetTime
	return diff >= int64(interval.Seconds())
}

func (s *State) LastResetTime() int64 {
	return s.lastResetTime
}

func (s *State) LastSeenTime() int64 {
	return s.lastSeenTime
}

func (s *State) Hits() int {
	return s.hits
}

func (s *State) HitIncr() {
	s.hits++
}

func (s *State) UpdateLastSeenTime() {
	s.lastSeenTime = s.now
}

func (s *State) UpdateLastResetTime() {
	s.lastResetTime = s.now
}

func (s *State) LastVisit(objectID string) int64 {
	visit, found := s.objectsVisits[objectID]
	if found {
		return visit
	}
	return 0
}

func (s *State) SetLastVisit(objectID string, visit int64) {
	s.objectsVisits[objectID] = visit
}

func NewState(id StateID) *State {
	return &State{
		id:            id,
		objectsVisits: make(map[string]int64),
	}
}

func (s StateID) validate() error {
	if len(s.IMEI) == 0 {
		return fmt.Errorf("spinix/state: imei not specified")
	}
	if len(s.RuleID) == 0 {
		return fmt.Errorf("spinix/state: rule regionFromLatLon not specified")
	}
	return nil
}

type stateByValueIndex []*stateByValueBucket

func (i stateByValueIndex) bucket(id string) *stateByValueBucket {
	return i[bucket(id, numBucket/2)]
}

func (i stateByValueIndex) remove(rootID string, id string) {
	bucket := i.bucket(rootID)
	bucket.Lock()
	defer bucket.Unlock()
	delete(bucket.index[rootID], id)
	if len(bucket.index[rootID]) == 0 {
		delete(bucket.index, rootID)
	}
}

func (i stateByValueIndex) add(rootID string, id string, s *State) {
	bucket := i.bucket(rootID)
	bucket.Lock()
	if bucket.index[rootID] == nil {
		bucket.index[rootID] = make(map[string]*State)
	}
	bucket.index[rootID][id] = s
	bucket.Unlock()
}

func (i stateByValueIndex) deleteAndIter(rootID string, fn func(s *State) error) error {
	bucket := i.bucket(rootID)
	bucket.Lock()
	for id, st := range bucket.index[rootID] {
		if err := fn(st); err != nil {
			return err
		}
		delete(bucket.index[rootID], id)
	}
	if len(bucket.index[rootID]) == 0 {
		delete(bucket.index, rootID)
	}
	bucket.Unlock()
	return nil
}

type stateByValueBucket struct {
	index map[string]map[string]*State
	sync.RWMutex
}

type stateIndex []*stateBucket

func (i stateIndex) get(id StateID) (*State, error) {
	bucket := i.bucket(id)
	bucket.RLock()
	defer bucket.RUnlock()
	state, ok := bucket.index[id]
	if !ok {
		return nil, fmt.Errorf("%w - %v", ErrStateNotFound, id)
	}
	return state, nil
}

func (i stateIndex) remove(id StateID) (err error) {
	bucket := i.bucket(id)
	bucket.Lock()
	defer bucket.Unlock()
	delete(bucket.index, id)
	return nil
}

func (i stateIndex) update(s *State) error {
	bucket := i.bucket(s.ID())
	bucket.Lock()
	defer bucket.Unlock()
	bucket.index[s.ID()] = s
	return nil
}

func (i stateIndex) add(s *State) {
	bucket := i.bucket(s.ID())
	bucket.Lock()
	defer bucket.Unlock()
	bucket.index[s.ID()] = s
}

func (i stateIndex) bucket(id StateID) *stateBucket {
	return i[bucket(id.String(), numBucket)]
}

type stateBucket struct {
	index map[StateID]*State
	sync.RWMutex
}
