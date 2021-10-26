package spinix

import (
	"sync/atomic"
)

// TODO: by each region
type StatsCollector struct {
	totalRules   uint64
	totalDevices uint64
	totalObjects uint64
	totalHits    uint64
	totalDetects uint64
}

type Stats struct {
	Rules   uint64
	Devices uint64
	Objects uint64
	Hits    uint64
	Detects uint64
}

func NewStatsCollector() *StatsCollector {
	return &StatsCollector{}
}

func (sc *StatsCollector) Stats() Stats {
	return Stats{
		Rules:   atomic.LoadUint64(&sc.totalRules),
		Devices: atomic.LoadUint64(&sc.totalDevices),
		Objects: atomic.LoadUint64(&sc.totalObjects),
		Hits:    atomic.LoadUint64(&sc.totalHits),
		Detects: atomic.LoadUint64(&sc.totalDetects),
	}
}

func (sc *StatsCollector) Reset() {
	atomic.StoreUint64(&sc.totalRules, 0)
	atomic.StoreUint64(&sc.totalDevices, 0)
	atomic.StoreUint64(&sc.totalObjects, 0)
	atomic.StoreUint64(&sc.totalHits, 0)
	atomic.StoreUint64(&sc.totalDetects, 0)
}

func (sc *StatsCollector) IncrRules() {
	atomic.AddUint64(&sc.totalRules, 1)
}

func (sc *StatsCollector) DecrRules() {
	atomic.AddUint64(&sc.totalRules, ^uint64(0))
}

func (sc *StatsCollector) IncrObjects() {
	atomic.AddUint64(&sc.totalObjects, 1)
}

func (sc *StatsCollector) DecrObjects() {
	atomic.AddUint64(&sc.totalObjects, ^uint64(0))
}

func (sc *StatsCollector) IncrDevices() {
	atomic.AddUint64(&sc.totalDevices, 1)
}

func (sc *StatsCollector) DecrDevices() {
	atomic.AddUint64(&sc.totalDevices, ^uint64(0))
}

func (sc *StatsCollector) IncrHits() {
	atomic.AddUint64(&sc.totalHits, 1)
}

func (sc *StatsCollector) IncrDetects() {
	atomic.AddUint64(&sc.totalDetects, 1)
}
