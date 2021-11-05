package spinix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/xid"

	"github.com/tidwall/geojson/geometry"
)

var ErrRuleNotFound = errors.New("spinix/rule: rule not found")

type Rules interface {
	Walk(ctx context.Context, lat float64, lon float64, fn RuleIterFunc) error
	Insert(ctx context.Context, r *Rule) error
	Delete(ctx context.Context, id RuleID) error
	Lookup(ctx context.Context, id RuleID) (*Rule, error)
}

type RuleID = xid.ID

type RuleIterFunc func(ctx context.Context, rule *Rule, err error) error

type Rule struct {
	id         RuleID
	specStr    string
	spec       *spec
	bbox       geometry.Rect
	regions    []RegionID
	regionSize RegionSize
}

func (r *Rule) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Snapshot())
}

func (r *Rule) UnmarshalJSON(data []byte) (err error) {
	var snap RuleSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	expr, err := ParseSpec(snap.Spec)
	if err != nil {
		return err
	}
	ruleSpec, err := exprToSpec(expr)
	if err != nil {
		return err
	}
	if ruleSpec.props.radius < 1000 {
		ruleSpec.props.radius = 1000
	}
	size := RegionSize(snap.RegionSize)
	regions := make([]RegionID, len(snap.RegionIDs))
	for i := 0; i < len(snap.RegionIDs); i++ {
		rid, err := RegionIDFromString(snap.RegionIDs[i])
		if err != nil {
			return err
		}
		regions[i] = rid
	}
	normalizeDistance(ruleSpec.props.radius, size)
	if ruleSpec.props.center.X == 0 && ruleSpec.props.center.Y == 0 {
		return fmt.Errorf("spinix/rule: center of the rule is not specified")
	}
	id, err := xid.FromString(snap.RuleID)
	if err != nil {
		return err
	}
	r.id = id
	r.regions = regions
	r.regionSize = size
	r.specStr = expr.String()
	r.spec = ruleSpec
	if err := r.calc(); err != nil {
		return err
	}
	return
}

func (r *Rule) calc() error {
	circle, bbox := makeCircle(
		r.spec.props.center.X,
		r.spec.props.center.Y,
		r.spec.props.radius,
		steps)
	r.regionSize = RegionSizeFromMeters(r.spec.props.radius)
	r.spec.normalizeRadius(r.regionSize)
	if err := r.regionSize.Validate(); err != nil {
		return err
	}
	r.regions = RegionIDs(circle, r.regionSize)
	r.bbox = bbox
	return nil
}

func (r *Rule) RegionSize() RegionSize {
	return r.regionSize
}

func (r *Rule) RegionIDs() (ids []RegionID) {
	ids = make([]RegionID, len(r.regions))
	copy(ids, r.regions)
	return ids
}

func (r *Rule) Regions() []Region {
	regions := make([]Region, len(r.regions))
	for ri, rid := range r.regions {
		regions[ri] = MakeRegion(rid, r.regionSize)
	}
	return regions
}

func (r *Rule) Bounding() geometry.Rect {
	return r.bbox
}

func (r *Rule) Center() geometry.Point {
	return r.spec.props.center
}

func (r *Rule) Specification() string {
	return r.specStr
}

func (r *Rule) ID() RuleID {
	return r.id
}

func (r *Rule) RefIDs() (refs map[xid.ID]Token) {
	for _, n := range r.spec.nodes {
		nodeRef := n.refIDs()
		if nodeRef == nil {
			continue
		}
		if refs == nil {
			refs = make(map[xid.ID]Token)
		}
		for k, v := range nodeRef {
			refs[k] = v
		}
	}
	return refs
}

func RuleFromSpec(id xid.ID, regions []RegionID, size RegionSize, spec string) (*Rule, error) {
	expr, err := ParseSpec(spec)
	if err != nil {
		return nil, err
	}
	ruleSpec, err := exprToSpec(expr)
	if err != nil {
		return nil, err
	}
	ruleSpec.normalizeRadius(size)
	if err := ruleSpec.validate(); err != nil {
		return nil, err
	}
	rule := &Rule{id: id}
	rule.regions = regions
	rule.regionSize = size
	rule.specStr = expr.String()
	rule.spec = ruleSpec
	if err := rule.calc(); err != nil {
		return nil, err
	}
	return rule, nil
}

func NewRule(spec string) (*Rule, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("spinix/rule: specification too short")
	}
	if len(spec) > 2048 {
		return nil, fmt.Errorf("spinix/rule: specification too long")
	}
	expr, err := ParseSpec(spec)
	if err != nil {
		return nil, err
	}
	ruleSpec, err := exprToSpec(expr)
	if err != nil {
		return nil, err
	}
	rule := &Rule{
		id:      xid.New(),
		spec:    ruleSpec,
		specStr: expr.String(),
	}
	if err := rule.calc(); err != nil {
		return nil, err
	}
	return rule, nil
}

type RuleSnapshot struct {
	RuleID     string   `json:"ID"`
	Spec       string   `json:"spec"`
	RegionIDs  []string `json:"RegionIDs"`
	RegionSize int      `json:"regionSize"`
}

func (r *Rule) Snapshot() RuleSnapshot {
	snapshot := RuleSnapshot{
		RuleID:     r.id.String(),
		Spec:       r.specStr,
		RegionIDs:  make([]string, len(r.regions)),
		RegionSize: r.regionSize.Value(),
	}
	for i := 0; i < len(r.regions); i++ {
		snapshot.RegionIDs[i] = r.regions[i].String()
	}
	return snapshot
}

func NewMemoryRules() Rules {
	return &rules{
		indexByRules:      newRuleIndex(),
		smallRegionsCells: make(map[RegionID]*regionCell),
		largeRegionsCells: make(map[RegionID]*regionCell),
	}
}

func (r *rules) Walk(ctx context.Context, lat float64, lon float64, fn RuleIterFunc) error {
	regionID := RegionFromLatLon(lat, lon, SmallRegionSize)
	r.RLock()
	region, ok := r.smallRegionsCells[regionID]
	r.RUnlock()
	if ok {
		if err := region.walk(ctx, lat, lon, fn); err != nil {
			return err
		}
	}
	regionID = RegionFromLatLon(lat, lon, LargeRegionSize)
	r.RLock()
	region, ok = r.largeRegionsCells[regionID]
	r.RUnlock()
	if ok {
		if err := region.walk(ctx, lat, lon, fn); err != nil {
			return err
		}
	}
	return nil
}

func (r *rules) Insert(_ context.Context, rule *Rule) error {
	if rule == nil {
		return fmt.Errorf("spinix/rule: not specified")
	}

	if err := rule.spec.validate(); err != nil {
		return err
	}

	var region *regionCell
	var found bool

	for _, regionID := range rule.regions {
		switch rule.regionSize {
		case SmallRegionSize:
			r.RLock()
			region, found = r.smallRegionsCells[regionID]
			r.RUnlock()
			if !found {
				region = newRegionCell(regionID, rule.regionSize)
				found = true
				r.Lock()
				r.smallRegionsCells[regionID] = region
				r.Unlock()
			}
		case LargeRegionSize:
			r.RLock()
			region, found = r.largeRegionsCells[regionID]
			r.RUnlock()
			if !found {
				region = newRegionCell(regionID, rule.regionSize)
				found = true
				r.Lock()
				r.largeRegionsCells[regionID] = region
				r.Unlock()
			}
		}
		if region != nil && found {
			region.insert(rule)
		}
		region = nil
	}
	return r.indexByRules.set(rule)
}

func (r *rules) Delete(_ context.Context, id RuleID) error {
	rule, err := r.indexByRules.get(id)
	if err != nil {
		return err
	}
	var region *regionCell
	var found bool
	for _, regionID := range rule.regions {
		switch rule.regionSize {
		case SmallRegionSize:
			r.RLock()
			region, found = r.smallRegionsCells[regionID]
			r.RUnlock()
		case LargeRegionSize:
			r.RLock()
			region, found = r.largeRegionsCells[regionID]
			r.RUnlock()
		}
		if region == nil || !found {
			continue
		}
		region.delete(rule)
		if region.isEmpty() {
			r.Lock()
			delete(r.smallRegionsCells, regionID)
			r.Unlock()
		}
		region = nil
	}
	return r.indexByRules.delete(id)
}

func (r *rules) Lookup(_ context.Context, id RuleID) (*Rule, error) {
	return r.indexByRules.get(id)
}

type rules struct {
	indexByRules      ruleIndex
	smallRegionsCells map[RegionID]*regionCell
	largeRegionsCells map[RegionID]*regionCell
	sync.RWMutex
}

type ruleIndex []*ruleBucket

func (i ruleIndex) get(id RuleID) (*Rule, error) {
	bucket := i.bucket(id)
	bucket.RLock()
	defer bucket.RUnlock()
	rule, ok := bucket.index[id]
	if !ok {
		return nil, fmt.Errorf("%w - %s", ErrRuleNotFound, id)
	}
	return rule, nil
}

func (i ruleIndex) set(rule *Rule) error {
	bucket := i.bucket(rule.ID())
	bucket.Lock()
	defer bucket.Unlock()
	_, ok := bucket.index[rule.ID()]
	if ok {
		return fmt.Errorf("spinix/rule: rule %s already refExists", rule.ID())
	}
	bucket.index[rule.ID()] = rule
	return nil
}

func (i ruleIndex) delete(id RuleID) error {
	bucket := i.bucket(id)
	bucket.Lock()
	defer bucket.Unlock()
	_, ok := bucket.index[id]
	if !ok {
		return fmt.Errorf("%w - %s", ErrRuleNotFound, id)
	}
	delete(bucket.index, id)
	return nil
}

func (i ruleIndex) bucket(id RuleID) *ruleBucket {
	return i[bucketFromID(id, numBucket)]
}

func newRuleIndex() ruleIndex {
	buckets := make([]*ruleBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &ruleBucket{
			index: make(map[RuleID]*Rule),
		}
	}
	return buckets
}

type ruleBucket struct {
	index map[RuleID]*Rule
	sync.RWMutex
}
