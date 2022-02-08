package tracker

import (
	"context"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/mmadfox/spinix/internal/geojson"

	"github.com/uber/h3-go/v3"
)

type Service interface {
	Add(ctx context.Context, object GeoJSON) ([]h3.H3Index, error)
	Remove(ctx context.Context, objectID uint64, index []h3.H3Index) error
	Detect(ctx context.Context) (events []Event, ok bool, err error)
}

type service struct {
	cluster Cluster
	proxy   Proxy
}

func NewService(c Cluster, proxy Proxy) Service {
	return &service{
		cluster: c,
		proxy:   proxy,
	}
}

func (s *service) Add(ctx context.Context, object GeoJSON) (cells []h3.H3Index, err error) {
	if err = object.Validate(); err != nil {
		return
	}

	// coordinator
	if object.HasNotIndex() {
		index, err := geojson.EnsureIndex(object.Data, s.cluster, s.cluster.CurrentLevel())
		if err != nil {
			return nil, err
		}
		object.Index = index.ByHost(s.cluster.Addr())
		if err := s.addObjectForEachHost(ctx, index, object); err != nil {
			return nil, err
		}
		return index.Cells(), nil
	}

	return object.Index, nil
}

func (s *service) Remove(ctx context.Context, objectID uint64, index []h3.H3Index) error {
	return nil
}

func (s *service) Detect(ctx context.Context) (events []Event, ok bool, err error) {
	return
}

func (s *service) addObjectForEachHost(ctx context.Context, index *geojson.Index, o GeoJSON) error {
	var group errgroup.Group
	sem := semaphore.NewWeighted(int64(runtime.NumCPU()))
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_ = index.ForEachHost(func(addr string, cells []h3.H3Index) error {
		if s.cluster.IsOwner(addr) {
			return nil
		}
		group.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			client, err := s.proxy.NewClient(ctx, addr)
			if err != nil {
				return err
			}
			newObj := o
			newObj.Index = cells
			_, err = client.Add(ctx, newObj)
			return err
		})
		return nil
	})
	return group.Wait()
}
