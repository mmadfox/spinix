package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	id        StateID
	LastSeen  int64
	NumOfHits int
	Objects   map[string]int64
}

func (s *State) Reset() {
	s.LastSeen = 0
	s.NumOfHits = 0
}

func (s *State) ID() StateID {
	return s.id
}

func NewState(id StateID) *State {
	return &State{
		id:      id,
		Objects: make(map[string]int64),
	}
}

func (s StateID) validate() error {
	if len(s.IMEI) == 0 {
		return fmt.Errorf("spinix/state: imei not specified")
	}
	if len(s.RuleID) == 0 {
		return fmt.Errorf("spinix/state: rule id not specified")
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
