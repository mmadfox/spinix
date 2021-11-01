package spinix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"
)

var ErrStateNotFound = errors.New("spinix/states:  not found")

type States interface {
	Lookup(ctx context.Context, id StateID) (*State, error)
	Update(ctx context.Context, s *State) error
	Make(ctx context.Context, id StateID) (*State, error)
	Remove(ctx context.Context, id StateID) error
	RemoveByRule(ctx context.Context, rid RuleID) error
	RemoveByDevice(ctx context.Context, did DeviceID) error
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
	ms.indexByRule.add(sid.rid, sid.did, state)
	ms.indexByDevice.add(sid.did, sid.rid, state)
	return state, nil
}

func (ms *memoryState) Remove(_ context.Context, k StateID) error {
	if err := ms.indexByID.remove(k); err != nil {
		return err
	}
	ms.indexByDevice.remove(k.did, k.rid)
	ms.indexByRule.remove(k.rid, k.did)
	return nil
}

func (ms *memoryState) RemoveByRule(_ context.Context, rid RuleID) (err error) {
	err = ms.indexByRule.deleteAndIter(rid, func(s *State) error {
		if err := ms.indexByID.remove(s.ID()); err != nil {
			return err
		}
		ms.indexByDevice.remove(s.DeviceID(), s.RuleID())
		return nil
	})
	return
}

func (ms *memoryState) RemoveByDevice(_ context.Context, did DeviceID) (err error) {
	err = ms.indexByDevice.deleteAndIter(did, func(s *State) error {
		if err := ms.indexByID.remove(s.ID()); err != nil {
			return err
		}
		ms.indexByRule.remove(s.RuleID(), s.DeviceID())
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
			index: make(map[xid.ID]map[xid.ID]*State),
		}
	}
	return buckets
}

type StateID struct {
	did DeviceID
	rid RuleID
}

func (s StateID) String() string {
	return s.did.String() + ":" + s.rid.String()
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
	ID            StateID          `json:"ID"`
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

func (s *State) DeviceID() DeviceID {
	return s.id.did
}

func (s *State) RuleID() RuleID {
	return s.id.rid
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
	if s.did.IsNil() {
		return fmt.Errorf("spinix/state: deviceID not specified")
	}
	if s.rid.IsNil() {
		return fmt.Errorf("spinix/state: ruleID not specified")
	}
	return nil
}

type stateByValueIndex []*stateByValueBucket

func (i stateByValueIndex) bucket(id xid.ID) *stateByValueBucket {
	return i[bucketFromID(id, numBucket/2)]
}

func (i stateByValueIndex) remove(rootID xid.ID, id xid.ID) {
	bucket := i.bucket(rootID)
	bucket.Lock()
	defer bucket.Unlock()
	delete(bucket.index[rootID], id)
	if len(bucket.index[rootID]) == 0 {
		delete(bucket.index, rootID)
	}
}

func (i stateByValueIndex) add(rootID xid.ID, id xid.ID, s *State) {
	bucket := i.bucket(rootID)
	bucket.Lock()
	if bucket.index[rootID] == nil {
		bucket.index[rootID] = make(map[xid.ID]*State)
	}
	bucket.index[rootID][id] = s
	bucket.Unlock()
}

func (i stateByValueIndex) deleteAndIter(rootID xid.ID, fn func(s *State) error) error {
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
	index map[xid.ID]map[xid.ID]*State
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
