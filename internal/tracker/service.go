package tracker

import (
	"context"
	"fmt"
	"log"

	"github.com/tidwall/geojson"

	geojsonindexer "github.com/mmadfox/spinix/internal/geojson"

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

func NewService(c Cluster) *service {
	return &service{
		cluster: c,
	}
}

func (s *service) AddGeoJSON(ctx context.Context, object GeoJSON) ([]h3.H3Index, error) {
	if err := object.Validate(); err != nil {
		return nil, err
	}

	if object.HasNotIndex() {
		index, err := s.ensureIndex(object.Data)
		if err != nil {
			return nil, err
		}
		object.Index = index
	}

	hosts, err := s.groupHostsByIndex(object.Index)
	if err != nil {
		return nil, err
	}

	for addr, index := range hosts {
		if s.cluster.IsOwner(addr) {
			log.Println("owner", addr, index)
		} else {
			log.Println("proxyTo", addr, index)
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

func (s *service) ensureIndex(object geojson.Object) ([]h3.H3Index, error) {
	index := geojsonindexer.EnsureIndex(object, s.cluster.CurrentLevel())
	if len(index) == 0 {
		return nil, ErrNoIndexData
	}
	return index, nil
}

func (s *service) groupHostsByIndex(index []h3.H3Index) (map[string][]h3.H3Index, error) {
	hosts := make(map[string][]h3.H3Index)
	for i := 0; i < len(index); i++ {
		dcell, found := s.cluster.Lookup(index[i])
		if !found {
			return nil, fmt.Errorf("could not find host for index %v",
				index[i])
		}
		_, ok := hosts[dcell.Host]
		if !ok {
			hosts[dcell.Host] = make([]h3.H3Index, 0, 2)
		}
		hosts[dcell.Host] = append(hosts[dcell.Host], dcell.H3ID)
	}
	return hosts, nil
}
