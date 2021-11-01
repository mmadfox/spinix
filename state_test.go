package spinix

//func populateStates(t *testing.T) (States, []StateID) {
//	storage := NewMemoryState()
//	devices := []string{"one", "two", "three"}
//	rules := []string{"rule1", "rule2", "rule3", "rule4"}
//	ctx := context.TODO()
//	ids := make([]StateID, 0)
//	for _, device := range devices {
//		for _, rule := range rules {
//			ID := StateID{IMEI: device, RuleID: rule}
//			state, err := storage.Make(ctx, ID)
//			if err != nil {
//				t.Fatal(err)
//			}
//			if have, want := state.ID().IMEI, state.ID().IMEI; have != want {
//				t.Fatalf("storage.Make(%s) => %v, want %v", ID, state.ID(), ID)
//			}
//			if have, want := state.ID().RuleID, state.ID().RuleID; have != want {
//				t.Fatalf("storage.Make(%s) => %v, want %v", ID, state.ID(), ID)
//			}
//			ids = append(ids, ID)
//		}
//	}
//	return storage, ids
//}
//
//func TestResetState(t *testing.T) {
//	minutes := 300
//	want := minutes / 5
//	state := NewState(StateID{IMEI: "one", RuleID: "one"})
//	startTime := time.Now().Add(-time.Duration(minutes) * time.Minute)
//	fiveMin := 5 * time.Minute
//	var have int
//	for i := 0; i < minutes; i++ {
//		startTime = startTime.Add(time.Minute)
//		state.SetTime(startTime.Unix())
//		if state.NeedReset(fiveMin) {
//			have++
//			state.UpdateLastResetTime()
//		}
//		state.UpdateLastSeenTime()
//		state.HitIncr()
//	}
//	if have != want {
//		t.Fatalf("state.NeedReset(%v) => %d, want %d", fiveMin, have, want)
//	}
//	if have, want := state.Hits(), minutes; have != want {
//		t.Fatalf("state.Hits() => %d, want %d", have, want)
//	}
//}
//
//func TestMemoryStateLookup(t *testing.T) {
//	storage, ids := populateStates(t)
//	ctx := context.TODO()
//	for _, ID := range ids {
//		state, err := storage.Lookup(ctx, ID)
//		if err != nil {
//			t.Fatal(err)
//		}
//		if have, want := state.ID().IMEI, ID.IMEI; have != want {
//			t.Fatalf("storage.Make(%s) => %v, want %v", ID, state.ID(), ID)
//		}
//		if have, want := state.ID().RuleID, ID.RuleID; have != want {
//			t.Fatalf("storage.Make(%s) => %v, want %v", ID, state.ID(), ID)
//		}
//	}
//	_, err := storage.Lookup(ctx, StateID{})
//	if !errors.Is(err, ErrStateNotFound) {
//		t.Fatalf("storage.Lookup(%v) => nil, want error", StateID{})
//	}
//}
//
//func TestMemoryStateRemove(t *testing.T) {
//	storage, ids := populateStates(t)
//	ctx := context.TODO()
//	for _, ID := range ids {
//		if err := storage.Remove(ctx, ID); err != nil {
//			t.Fatal(err)
//		}
//	}
//	for _, ID := range ids {
//		_, err := storage.Lookup(ctx, ID)
//		if !errors.Is(err, ErrStateNotFound) {
//			t.Fatalf("storage.Lookup(%v) => nil, want error", ID)
//		}
//	}
//}
//
//func TestMemoryStateRemoveByRules(t *testing.T) {
//	storage, ids := populateStates(t)
//	ctx := context.TODO()
//	for _, ID := range ids {
//		if err := storage.RemoveByRule(ctx, ID.RuleID); err != nil {
//			t.Fatal(err)
//		}
//	}
//	for _, ID := range ids {
//		_, err := storage.Lookup(ctx, ID)
//		if !errors.Is(err, ErrStateNotFound) {
//			t.Fatalf("storage.Lookup(%v) => nil, want error", ID)
//		}
//	}
//}
//
//func TestMemoryStateRemoveByDevice(t *testing.T) {
//	storage, ids := populateStates(t)
//	ctx := context.TODO()
//	for _, ID := range ids {
//		if err := storage.RemoveByDevice(ctx, ID.IMEI); err != nil {
//			t.Fatal(err)
//		}
//	}
//	for _, ID := range ids {
//		_, err := storage.Lookup(ctx, ID)
//		if !errors.Is(err, ErrStateNotFound) {
//			t.Fatalf("storage.Lookup(%v) => nil, want error", ID)
//		}
//	}
//}
