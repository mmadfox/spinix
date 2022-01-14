package cluster

type Cluster struct {
	nodeManager *nodeman
	router      *router
	client      *client
	balancer    *balancer
}

func New(opts *Options) (*Cluster, error) {
	return nil, nil
}
