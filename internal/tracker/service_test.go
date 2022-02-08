package tracker_test

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uber/h3-go/v3"

	"github.com/mmadfox/spinix/internal/tracker"

	"github.com/mmadfox/geojson"

	"github.com/golang/mock/gomock"
	mocktracker "github.com/mmadfox/spinix/mocks/tracker"
)

func TestTrackerService_AddWithCoordinatorRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	proxy := mocktracker.NewMockProxy(ctrl)
	client := mocktracker.NewMockService(ctrl)
	lc := tracker.NewLocalCluster("127.0.0.1", 3)
	lc.AddHost("127.0.0.2")
	lc.AddHost("127.0.0.3")
	lc.AddHost("127.0.0.4")

	object1 := tracker.GeoJSON{
		ObjectID: 123,
		LayerID:  555,
		Data:     loadDataFromFile(t, "./testdata/geom1.json"),
	}
	client.EXPECT().Add(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, o tracker.GeoJSON) ([]h3.H3Index, error) {
			time.Sleep(time.Second)
			assert.NotEmpty(t, o.Index)
			assert.Equal(t, object1.ObjectID, o.ObjectID)
			assert.Equal(t, object1.LayerID, o.LayerID)
			return o.Index, nil
		}).Times(3)

	proxy.EXPECT().NewClient(gomock.Any(),
		oneOf([]string{
			"127.0.0.2", "127.0.0.3", "127.0.0.4",
		}),
	).DoAndReturn(func(ctx context.Context, addr string) (tracker.Service, error) {
		return client, nil
	}).AnyTimes()

	service := tracker.NewService(lc, proxy)

	index, err := service.Add(ctx, object1)
	assert.NoError(t, err)
	assert.NotEmpty(t, index)
}

type oneOfMatcher struct {
	values []string
}

func oneOf(values []string) gomock.Matcher {
	return oneOfMatcher{
		values: values,
	}
}

func (m oneOfMatcher) String() string {
	return strings.Join(m.values, ", ")
}

func (m oneOfMatcher) Matches(arg interface{}) bool {
	str := arg.(string)
	for _, val := range m.values {
		if strings.Contains(str, val) {
			return true
		}
	}
	return false
}

func loadDataFromFile(t *testing.T, filename string) geojson.Object {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	object, err := geojson.Parse(string(data), geojson.DefaultParseOptions)
	if err != nil {
		t.Fatal(err)
	}
	return object
}
