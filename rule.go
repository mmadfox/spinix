package spinix

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/xid"

	"github.com/tidwall/rtree"

	"github.com/uber/h3-go"

	"github.com/google/btree"

	"github.com/tidwall/geojson/geometry"
)

const (
	smallLevel        = 2
	largeLevel        = 0
	minRadiusInMeters = 500
	maxRadiusInMeters = 100000
)

type Rules interface {
	Walk(ctx context.Context, device *Device, fn WalkRuleFunc) error
	Insert(ctx context.Context, r *Rule) error
	Delete(ctx context.Context, ruleID string) error
	FindOne(ctx context.Context, ruleID string) (*Rule, error)
	Find(ctx context.Context, f RulesFilter) ([]*Rule, error)
}

type WalkRuleFunc func(ctx context.Context, rule *Rule, err error) error

type RulesFilter struct {
}

type Rule struct {
	ruleID       string
	name         string
	expr         Expr
	spec         string
	meters       float64
	bbox         geometry.Rect
	referenceIDs []string
	regionIDs    []h3.H3Index
	regionLevel  int
	searchRadius []geometry.Point
	center       geometry.Point
}

func (r *Rule) validate() error {
	if len(r.ruleID) == 0 {
		return fmt.Errorf("georule/rule: id not specified")
	}
	if len(r.name) == 0 {
		return fmt.Errorf("georule/rule: %s name not specified", r.ruleID)
	}
	if r.expr == nil || len(r.spec) == 0 {
		return fmt.Errorf("georule/rule: %s spec not specified", r.ruleID)
	}
	if r.meters < minRadiusInMeters {
		return fmt.Errorf("georule/rule: %s search radius is less than %d meters",
			r.ruleID, minRadiusInMeters)
	}
	if len(r.regionIDs) == 0 {
		return fmt.Errorf("georule/rule: %s regionIDs not specified", r.ruleID)
	}
	return nil
}

func NewRule(
	name string,
	spec string,
	centerLat float64,
	centerLon float64,
	radiusInMeters float64,
) (*Rule, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("georule: specification too short")
	}
	if len(spec) > 1024 {
		return nil, fmt.Errorf("georule: specification too long")
	}

	if len(name) == 0 {
		return nil, fmt.Errorf("georule: name too short")
	}
	if len(name) > 180 {
		return nil, fmt.Errorf("georule: name too long")
	}
	if radiusInMeters < minRadiusInMeters {
		radiusInMeters = minRadiusInMeters
	}
	if radiusInMeters > 100000000 {
		radiusInMeters = 100000000
	}

	expr, err := ParseSpec(spec)
	if err != nil {
		return nil, err
	}

	steps := getSteps(radiusInMeters)
	regionLevel := getLevel(radiusInMeters)
	circle, bbox := newCircle(centerLat, centerLon, radiusInMeters, steps)
	regionIDs := cover(radiusInMeters, regionLevel, circle)

	return &Rule{
		ruleID:      xid.New().String(),
		name:        name,
		expr:        expr,
		spec:        spec,
		center:      geometry.Point{X: centerLat, Y: centerLon},
		meters:      radiusInMeters,
		bbox:        bbox,
		regionIDs:   regionIDs,
		regionLevel: regionLevel,
	}, nil
}

type RuleSnapshot struct {
	RuleID       string   `json:"ruleID"`
	Name         string   `json:"name"`
	Spec         string   `json:"spec"`
	Latitude     float64  `json:"lat"`
	Longitude    float64  `json:"lon"`
	RadiusMeters float64  `json:"radiusMeters"`
	RegionIDs    []uint64 `json:"regionIDs"`
	RegionLevel  int      `json:"regionLevel"`
}

func TakeRuleSnapshot(r *Rule) RuleSnapshot {
	snapshot := RuleSnapshot{
		RuleID:       r.ruleID,
		Name:         r.name,
		Spec:         r.spec,
		Latitude:     r.center.X,
		Longitude:    r.center.Y,
		RadiusMeters: r.meters,
		RegionLevel:  r.regionLevel,
		RegionIDs:    make([]uint64, len(r.regionIDs)),
	}
	for i := 0; i < len(r.regionIDs); i++ {
		snapshot.RegionIDs[i] = uint64(r.regionIDs[i])
	}
	return snapshot
}

func (r Rule) ID() string {
	return r.ruleID
}

func (r Rule) Expr() Expr {
	return r.expr
}

func (r Rule) Bounds() geometry.Rect {
	return r.bbox
}

func (r Rule) Less(b btree.Item) bool {
	return r.ruleID < b.(*Rule).ruleID
}

type Stats struct {
}

func NewMemoryRules() Rules {
	return &rules{
		smallRegionIndex: newSmallRegionIndex(),
		largeRegionIndex: newLargeRegionIndex(),
		ruleIndex:        newRuleIndex(),
	}
}

func (r *rules) Stats() (Stats, error) {
	return Stats{}, nil
}

func (r *rules) Walk(ctx context.Context, device *Device, fn WalkRuleFunc) (err error) {
	if err := r.walkSmallRegion(ctx, device, fn); err != nil {
		return err
	}
	return r.walkLargeRegion(ctx, device, fn)
}

func (r *rules) Insert(_ context.Context, rule *Rule) (err error) {
	switch rule.regionLevel {
	case smallLevel:
		err = r.insertToSmallRegion(rule)
	case largeLevel:
		err = r.insertToLargeRegion(rule)
	default:
		err = fmt.Errorf("georule/rules: region level %d not defined", rule.regionLevel)
	}
	if err == nil {
		r.ruleIndex.set(rule)
	}
	return
}

func (r *rules) Delete(_ context.Context, ruleID string) error {
	rule, err := r.ruleIndex.get(ruleID)
	if err != nil {
		return err
	}
	for _, regionID := range rule.regionIDs {
		switch rule.regionLevel {
		case smallLevel:
			region, ok := r.smallRegionIndex.find(regionID)
			if !ok {
				continue
			}
			region.delete(rule)
			if region.isEmpty() {
				r.smallRegionIndex.delete(regionID)
			}
		case largeLevel:
			region, ok := r.largeRegionIndex.find(regionID)
			if !ok {
				continue
			}
			region.delete(rule)
			if region.isEmpty() {
				r.largeRegionIndex.delete(regionID)
			}
		}
	}
	r.ruleIndex.delete(ruleID)
	return nil
}

func (r *rules) Find(ctx context.Context, f RulesFilter) ([]*Rule, error) {
	return nil, nil
}

func (r *rules) FindOne(_ context.Context, ruleID string) (*Rule, error) {
	return r.ruleIndex.get(ruleID)
}

func (r *rules) insertToLargeRegion(rule *Rule) error {
	for _, regionID := range rule.regionIDs {
		r.largeRegionIndex.findOrCreate(regionID).insertRule(rule)
	}
	return nil
}

func (r *rules) insertToSmallRegion(rule *Rule) error {
	for _, regionID := range rule.regionIDs {
		r.smallRegionIndex.findOrCreate(regionID).insertRule(rule)
	}
	return nil
}

func (r *rules) walkSmallRegion(ctx context.Context, device *Device, fn WalkRuleFunc) error {
	cord := h3.GeoCoord{Latitude: device.Latitude, Longitude: device.Longitude}
	regionID := h3.FromGeo(cord, smallLevel)
	region, ok := r.smallRegionIndex.find(regionID)
	if !ok {
		return nil
	}
	return region.walk(ctx, device, fn)
}

func (r *rules) walkLargeRegion(ctx context.Context, device *Device, fn WalkRuleFunc) error {
	cord := h3.GeoCoord{Latitude: device.Latitude, Longitude: device.Longitude}
	regionID := h3.FromGeo(cord, largeLevel)
	region, ok := r.largeRegionIndex.find(regionID)
	if !ok {
		return nil
	}
	return region.walk(ctx, device, fn)
}

type rules struct {
	counter          uint64
	smallRegionIndex *smallRegionIndex
	largeRegionIndex *largeRegionIndex
	ruleIndex        rulesIndex
}

type largeRegionIndex struct {
	index map[h3.H3Index]*ruleLargeRegion
	mu    sync.RWMutex
}

func (i *largeRegionIndex) find(id h3.H3Index) (*ruleLargeRegion, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	region, ok := i.index[id]
	if !ok {
		return nil, false
	}
	return region, true
}

func (i *largeRegionIndex) delete(id h3.H3Index) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.index, id)
}

func (i *largeRegionIndex) findOrCreate(id h3.H3Index) *ruleLargeRegion {
	i.mu.RLock()
	region, found := i.index[id]
	i.mu.RUnlock()
	if found {
		return region
	}
	region = newRuleLargeRegion(id)
	i.mu.Lock()
	i.index[id] = region
	i.mu.Unlock()
	return region
}

func newLargeRegionIndex() *largeRegionIndex {
	return &largeRegionIndex{
		index: make(map[h3.H3Index]*ruleLargeRegion),
	}
}

type smallRegionIndex struct {
	index map[h3.H3Index]*ruleSmallRegion
	mu    sync.RWMutex
}

func (i *smallRegionIndex) find(id h3.H3Index) (*ruleSmallRegion, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	region, ok := i.index[id]
	if !ok {
		return nil, false
	}
	return region, true
}

func (i *smallRegionIndex) delete(id h3.H3Index) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.index, id)
}

func (i *smallRegionIndex) findOrCreate(id h3.H3Index) *ruleSmallRegion {
	i.mu.RLock()
	region, found := i.index[id]
	i.mu.RUnlock()
	if found {
		return region
	}
	region = newRuleSmallRegion(id)
	i.mu.Lock()
	i.index[id] = region
	i.mu.Unlock()
	return region
}

func newSmallRegionIndex() *smallRegionIndex {
	return &smallRegionIndex{
		index: make(map[h3.H3Index]*ruleSmallRegion),
	}
}

type rulesIndex []*ruleBucket

const ruleBucketCount = 32

type ruleBucket struct {
	sync.RWMutex
	index map[string]*Rule
}

func newRuleIndex() rulesIndex {
	buckets := make([]*ruleBucket, ruleBucketCount)
	for i := 0; i < ruleBucketCount; i++ {
		buckets[i] = &ruleBucket{
			index: make(map[string]*Rule),
		}
	}
	return buckets
}

func (i rulesIndex) bucket(ruleID string) *ruleBucket {
	return i[bucket(ruleID, numBucket)]
}

func (i rulesIndex) set(rule *Rule) {
	bucket := i.bucket(rule.ruleID)
	bucket.Lock()
	bucket.index[rule.ruleID] = rule
	bucket.Unlock()
}

func (i rulesIndex) delete(ruleID string) {
	bucket := i.bucket(ruleID)
	bucket.Lock()
	delete(bucket.index, ruleID)
	bucket.Unlock()
}

func (i rulesIndex) get(ruleID string) (*Rule, error) {
	bucket := i.bucket(ruleID)
	bucket.RLock()
	defer bucket.RUnlock()
	rule, ok := bucket.index[ruleID]
	if !ok {
		return nil, fmt.Errorf("georule: rule %s not found", ruleID)
	}
	return rule, nil
}

type ruleSmallRegion struct {
	id      h3.H3Index
	mu      sync.RWMutex
	index   *rtree.RTree
	counter uint64
}

func newRuleSmallRegion(id h3.H3Index) *ruleSmallRegion {
	return &ruleSmallRegion{
		id:    id,
		index: &rtree.RTree{},
	}
}

func (r *ruleSmallRegion) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counter == 0
}

func (r *ruleSmallRegion) delete(rule *Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counter > 0 {
		r.counter--
	}
	r.index.Delete(
		[2]float64{rule.bbox.Min.X, rule.bbox.Min.Y},
		[2]float64{rule.bbox.Max.X, rule.bbox.Max.Y},
		rule,
	)
}

func (r *ruleSmallRegion) insertRule(rule *Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	bbox := rule.Bounds()
	r.counter++
	r.index.Insert(
		[2]float64{bbox.Min.X, bbox.Min.Y},
		[2]float64{bbox.Max.X, bbox.Max.Y},
		rule)
}

func (r *ruleSmallRegion) walk(ctx context.Context, device *Device, fn WalkRuleFunc) (err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.index.Search(
		[2]float64{device.Latitude, device.Longitude},
		[2]float64{device.Latitude, device.Longitude},
		func(_, _ [2]float64, value interface{}) bool {
			rule, ok := value.(*Rule)
			if ok {
				if err = fn(ctx, rule, nil); err != nil {
					return false
				}
			}
			return true
		},
	)
	return
}

type ruleLargeRegion struct {
	id    h3.H3Index
	mu    sync.RWMutex
	index map[string]*Rule
}

func newRuleLargeRegion(id h3.H3Index) *ruleLargeRegion {
	return &ruleLargeRegion{
		id:    id,
		index: make(map[string]*Rule),
	}
}

func (r *ruleLargeRegion) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.index) == 0
}

func (r *ruleLargeRegion) delete(rule *Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.index, rule.ruleID)
}

func (r *ruleLargeRegion) walk(ctx context.Context, _ *Device, fn WalkRuleFunc) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, rule := range r.index {
		if err := fn(ctx, rule, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *ruleLargeRegion) insertRule(rule *Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index[rule.ruleID] = rule
}

func (r *ruleLargeRegion) removeRule(ruleID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.index, ruleID)
}
