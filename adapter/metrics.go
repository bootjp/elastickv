package adapter

import "github.com/prometheus/client_golang/prometheus"

var (
	opCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "elastickv_operations_total",
		Help: "Total number of KV operations",
	}, []string{"op", "client"})
)

func init() {
	prometheus.MustRegister(opCounter)
}
