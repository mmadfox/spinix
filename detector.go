package georule

import (
	"context"

	"github.com/tidwall/geojson"
)

type Detector struct {
	v VarStorage
	r RuleStorage
	s StateStorage
}

func NewDetector(
	v VarStorage,
	r RuleStorage,
	s StateStorage,
) *Detector {
	return &Detector{v: v, r: r, s: s}
}

func (d *Detector) SetRule(ctx context.Context, id string, name string, rule string) (S, error) {
	spec, err := Spec(id, name, rule)
	if err != nil {
		return S{}, err
	}
	if err := d.r.Store(ctx, spec); err != nil {
		return S{}, err
	}
	return spec, err
}

func (d *Detector) LookupRule(ctx context.Context, id string) (S, error) {
	return d.r.Lookup(ctx, id)
}

func (d *Detector) RemoveRule(ctx context.Context, id string) error {
	return d.r.Delete(ctx, id)
}

func (d *Detector) SetVar(ctx context.Context, id string, o geojson.Object) error {
	return d.v.Set(ctx, id, o)
}

func (d *Detector) RemoveVar(ctx context.Context, id string) error {
	return d.v.Delete(ctx, id)
}

func (d *Detector) LookupVar(ctx context.Context, id string) (geojson.Object, error) {
	return d.v.Lookup(ctx, id)
}

func (d *Detector) Detect(ctx context.Context, device *Device) {
}
