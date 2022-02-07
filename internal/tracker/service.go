package tracker

import (
	"context"

	"github.com/mmadfox/spinix/internal/geojson"

	"github.com/uber/h3-go/v3"
)

type Service interface {
	AddGeoJSON(ctx context.Context, object GeoJSON) ([]h3.H3Index, error)
	RemoveGeoJSON(ctx context.Context, objectID uint64, index []h3.H3Index) error
	Detect(ctx context.Context) (events []Event, ok bool, err error)
}

type service struct {
	cluster Cluster
}

func NewService(c Cluster) Service {
	return &service{
		cluster: c,
	}
}

func (s *service) AddGeoJSON(ctx context.Context, object GeoJSON) ([]h3.H3Index, error) {
	if err := object.Validate(); err != nil {
		return nil, err
	}

	isCoordinator := object.HasNotIndex()
	if isCoordinator {
		index, err := geojson.EnsureIndex(object.Data, s.cluster, s.cluster.CurrentLevel())
		if err != nil {
			return nil, err
		}
		if err := index.ForEachHost(func(addr string, cells []h3.H3Index) error {
			if s.cluster.IsOwner(addr) {
				object.Index = cells
			} else {
				object.Index = cells
				// TODO: proxy to hosts ...
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (s *service) RemoveGeoJSON(ctx context.Context, objectID uint64, index []h3.H3Index) error {
	return nil
}

func (s *service) Detect(ctx context.Context) (events []Event, ok bool, err error) {
	return
}
