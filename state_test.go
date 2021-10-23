package spinix

import (
	"context"
	"errors"
	"testing"
)

func populateStates(t *testing.T) (States, []StateID) {
	storage := NewMemoryState()
	devices := []string{"one", "two", "three"}
	rules := []string{"rule1", "rule2", "rule3", "rule4"}
	ctx := context.TODO()
	ids := make([]StateID, 0)
	for _, device := range devices {
		for _, rule := range rules {
			id := StateID{IMEI: device, RuleID: rule}
			state, err := storage.Make(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if have, want := state.ID().IMEI, state.ID().IMEI; have != want {
				t.Fatalf("storage.Make(%s) => %v, want %v", id, state.ID(), id)
			}
			if have, want := state.ID().RuleID, state.ID().RuleID; have != want {
				t.Fatalf("storage.Make(%s) => %v, want %v", id, state.ID(), id)
			}
			ids = append(ids, id)
		}
	}
	return storage, ids
}

func TestMemoryStateLookup(t *testing.T) {
	storage, ids := populateStates(t)
	ctx := context.TODO()
	for _, id := range ids {
		state, err := storage.Lookup(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := state.ID().IMEI, id.IMEI; have != want {
			t.Fatalf("storage.Make(%s) => %v, want %v", id, state.ID(), id)
		}
		if have, want := state.ID().RuleID, id.RuleID; have != want {
			t.Fatalf("storage.Make(%s) => %v, want %v", id, state.ID(), id)
		}
	}
	_, err := storage.Lookup(ctx, StateID{})
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("storage.Lookup(%v) => nil, want error", StateID{})
	}
}

func TestMemoryStateRemove(t *testing.T) {
	storage, ids := populateStates(t)
	ctx := context.TODO()
	for _, id := range ids {
		if err := storage.Remove(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		_, err := storage.Lookup(ctx, id)
		if !errors.Is(err, ErrStateNotFound) {
			t.Fatalf("storage.Lookup(%v) => nil, want error", id)
		}
	}
}

func TestMemoryStateRemoveByRules(t *testing.T) {
	storage, ids := populateStates(t)
	ctx := context.TODO()
	for _, id := range ids {
		if err := storage.RemoveByRule(ctx, id.RuleID); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		_, err := storage.Lookup(ctx, id)
		if !errors.Is(err, ErrStateNotFound) {
			t.Fatalf("storage.Lookup(%v) => nil, want error", id)
		}
	}
}

func TestMemoryStateRemoveByDevice(t *testing.T) {
	storage, ids := populateStates(t)
	ctx := context.TODO()
	for _, id := range ids {
		if err := storage.RemoveByDevice(ctx, id.IMEI); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		_, err := storage.Lookup(ctx, id)
		if !errors.Is(err, ErrStateNotFound) {
			t.Fatalf("storage.Lookup(%v) => nil, want error", id)
		}
	}
}
