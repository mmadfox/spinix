package georule

import (
	"context"
	"testing"
)

func TestInMemVars_Lookup(t *testing.T) {
	vars := NewInMemVars()
	v, err := vars.Lookup(context.Background(), "polygon")
	if err == nil {
		t.Fatalf("should not be nil when item is missing from vars")
	}
	if v != nil {
		t.Fatalf("should be nil when item is missing from vars")
	}
}

func TestInMemVars_Set(t *testing.T) {
	ctx := context.Background()
	vars := NewInMemVars()
	if have, want := vars.Set(ctx, "key1", "value"), error(nil); have != want {
		t.Fatalf("vars.Set(key1, value) => %v, want %v", have, want)
	}
	if have, want := vars.Set(ctx, "key1", "value"), error(nil); have == want {
		t.Fatalf("vars.Set(key1, value) => %v, want %v", have, want)
	}
	v, err := vars.Lookup(context.Background(), "key1")
	if err != nil {
		t.Fatalf("should be nil")
	}
	if v == nil {
		t.Fatalf("should not be nil")
	}
	if have, want := v.(string), "value"; have != want {
		t.Fatalf("vars.Lookup(key1) => %s, want %s", have, want)
	}
}

func TestInMemVars_Remove(t *testing.T) {
	vars := NewInMemVars()
	ctx := context.Background()
	if have, want := vars.Set(ctx, "key1", "value"), error(nil); have != want {
		t.Fatalf("vars.Set(key1, value) => %v, want %v", have, want)
	}
	_ = vars.Remove(ctx, "key1")
	v, err := vars.Lookup(context.Background(), "key1")
	if err == nil {
		t.Fatalf("should not be nil")
	}
	if v != nil {
		t.Fatalf("should be nil")
	}
}
