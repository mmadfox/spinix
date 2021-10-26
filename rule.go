package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/xid"

	"github.com/tidwall/geojson/geometry"
)

var ErrRuleNotFound = errors.New("spinix/rule: rule not found")

type Rules interface {
	Walk(ctx context.Context, device *Device, fn WalkRuleFunc) error
	Insert(ctx context.Context, r *Rule) error
	Delete(ctx context.Context, ruleID string) error
	Lookup(ctx context.Context, ruleID string) (*Rule, error)
}

type WalkRuleFunc func(ctx context.Context, rule *Rule, err error) error

type Rule struct {
	ruleID     string
	spec       *spec
	bbox       geometry.Rect
	regions    []RegionID
	regionSize RegionSize
	circle     radiusRing
}

func (r *Rule) calc() error {
	circle, bbox := makeCircle(r.spec.center.X, r.spec.center.Y, r.spec.radius, steps)
	r.circle = radiusRing{points: circle, rect: bbox}
	r.regionSize = regionSizeFromMeters(r.spec.radius)
	if err := r.regionSize.Validate(); err != nil {
		return err
	}
	r.regions = regionIDs(circle, r.regionSize)
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

func (r *Rule) Circle() geometry.Series {
	return r.circle
}

func (r *Rule) Bounding() geometry.Rect {
	return r.bbox
}

func (r *Rule) Center() geometry.Point {
	return r.spec.center
}

func (r *Rule) ID() string {
	return r.ruleID
}

func (r *Rule) RefIDs() (refs map[string]Token) {
	for _, n := range r.spec.nodes {
		nodeRef := n.refIDs()
		if nodeRef == nil {
			continue
		}
		if refs == nil {
			refs = make(map[string]Token)
		}
		for k, v := range nodeRef {
			refs[k] = v
		}
	}
	return refs
}

func NewRule(spec string) (*Rule, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("spinix/rule: specification too short")
	}
	if len(spec) > 1024 {
		return nil, fmt.Errorf("spinix/rule: specification too long")
	}
	ruleSpec, err := specFromString(spec)
	if err != nil {
		return nil, err
	}
	if ruleSpec.radius < 1000 {
		ruleSpec.radius = 1000
	}
	if ruleSpec.radius > largeRegionThreshold {
		ruleSpec.radius = largeRegionThreshold
	}
	if ruleSpec.center.X == 0 && ruleSpec.center.Y == 0 {
		return nil, fmt.Errorf("spinix/rule: center coordinates of the rule is not specified")
	}
	rule := &Rule{
		ruleID: xid.New().String(),
		spec:   ruleSpec,
	}
	if err := rule.calc(); err != nil {
		return nil, err
	}
	return rule, nil
}

type RuleSnapshot struct {
	RuleID     string   `json:"ruleID"`
	Spec       string   `json:"spec"`
	RegionIDs  []string `json:"regionIDs"`
	RegionSize int      `json:"regionCellSize"`
}

func Snapshot(r *Rule) RuleSnapshot {
	snapshot := RuleSnapshot{
		RuleID:    r.ruleID,
		RegionIDs: make([]string, len(r.regions)),
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

func (r *rules) Walk(ctx context.Context, device *Device, fn WalkRuleFunc) error {
	regionID := regionFromLatLon(device.Latitude, device.Longitude, smallRegionSize)
	r.RLock()
	region, ok := r.smallRegionsCells[regionID]
	r.RUnlock()
	if ok {
		if err := region.walk(ctx, device.Latitude, device.Longitude, fn); err != nil {
			return err
		}
	}
	regionID = regionFromLatLon(device.Latitude, device.Longitude, largeRegionSize)
	r.RLock()
	region, ok = r.largeRegionsCells[regionID]
	r.RUnlock()
	if ok {
		if err := region.walk(ctx, device.Latitude, device.Longitude, fn); err != nil {
			return err
		}
	}
	return nil
}

func (r *rules) Insert(_ context.Context, rule *Rule) error {
	if rule == nil {
		return fmt.Errorf("spinix/rule: rule is nil pointer")
	}
	var region *regionCell
	var found bool
	for _, regionID := range rule.regions {
		switch rule.regionSize {
		case smallRegionSize:
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
		case largeRegionSize:
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

func (r *rules) Delete(_ context.Context, ruleID string) error {
	rule, err := r.indexByRules.get(ruleID)
	if err != nil {
		return err
	}
	var region *regionCell
	var found bool
	for _, regionID := range rule.regions {
		switch rule.regionSize {
		case smallRegionSize:
			r.RLock()
			region, found = r.smallRegionsCells[regionID]
			r.RUnlock()
		case largeRegionSize:
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
	return r.indexByRules.delete(ruleID)
}

func (r *rules) Lookup(_ context.Context, ruleID string) (*Rule, error) {
	return r.indexByRules.get(ruleID)
}

type rules struct {
	indexByRules      ruleIndex
	smallRegionsCells map[RegionID]*regionCell
	largeRegionsCells map[RegionID]*regionCell
	sync.RWMutex
}

type ruleIndex []*ruleBucket

func (i ruleIndex) get(ruleID string) (*Rule, error) {
	bucket := i.bucket(ruleID)
	bucket.RLock()
	defer bucket.RUnlock()
	rule, ok := bucket.index[ruleID]
	if !ok {
		return nil, fmt.Errorf("%w - %s", ErrRuleNotFound, ruleID)
	}
	return rule, nil
}

func (i ruleIndex) set(rule *Rule) error {
	bucket := i.bucket(rule.ID())
	bucket.Lock()
	defer bucket.Unlock()
	_, ok := bucket.index[rule.ID()]
	if ok {
		return fmt.Errorf("spinix/rule: rule %s already exists", rule.ID())
	}
	bucket.index[rule.ID()] = rule
	return nil
}

func (i ruleIndex) delete(ruleID string) error {
	bucket := i.bucket(ruleID)
	bucket.Lock()
	defer bucket.Unlock()
	_, ok := bucket.index[ruleID]
	if !ok {
		return fmt.Errorf("%w - %s", ErrRuleNotFound, ruleID)
	}
	delete(bucket.index, ruleID)
	return nil
}

func (i ruleIndex) bucket(ruleID string) *ruleBucket {
	return i[bucket(ruleID, numBucket)]
}

func newRuleIndex() ruleIndex {
	buckets := make([]*ruleBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &ruleBucket{
			index: make(map[string]*Rule),
		}
	}
	return buckets
}

type ruleBucket struct {
	index map[string]*Rule
	sync.RWMutex
}
