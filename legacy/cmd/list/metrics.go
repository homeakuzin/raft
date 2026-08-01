package main

import (
	"time"

	"github.com/homeakuzin/raft"
	"github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	reads            prometheus.Counter
	writes           prometheus.Counter
	readLatency      prometheus.Histogram
	writeLatency     prometheus.Histogram
	replicateLatency prometheus.Histogram
}

func newRaftMetrics(reg prometheus.Registerer, node *raft.Node, nodeID string) (*raftMetrics, error) {
	constLabels := prometheus.Labels{"node_id": nodeID}
	m := &raftMetrics{
		reads: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raft_reads_total",
			Help:        "Total number of HTTP read requests.",
			ConstLabels: constLabels,
		}),
		writes: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raft_writes_total",
			Help:        "Total number of HTTP write requests.",
			ConstLabels: constLabels,
		}),
		readLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "raft_read_duration_seconds",
			Help:        "Latency of HTTP read requests.",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
		writeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "raft_write_duration_seconds",
			Help:        "Latency of HTTP write requests.",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
		replicateLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "raft_replicate_duration_seconds",
			Help:        "Latency to replicate a write to quorum.",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
	}

	for _, collector := range []prometheus.Collector{
		m.reads,
		m.writes,
		m.readLatency,
		m.writeLatency,
		m.replicateLatency,
		newNodeStateCollector(node, constLabels),
	} {
		if err := reg.Register(collector); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *raftMetrics) observeRead(start time.Time) {
	m.reads.Inc()
	m.readLatency.Observe(time.Since(start).Seconds())
}

func (m *raftMetrics) observeWrite(start time.Time) {
	m.writes.Inc()
	m.writeLatency.Observe(time.Since(start).Seconds())
}

func (m *raftMetrics) observeReplicate(start time.Time) {
	m.replicateLatency.Observe(time.Since(start).Seconds())
}

type nodeStateCollector struct {
	node     *raft.Node
	roleDesc *prometheus.Desc
	termDesc *prometheus.Desc
	logsDesc *prometheus.Desc
}

func newNodeStateCollector(node *raft.Node, constLabels prometheus.Labels) prometheus.Collector {
	return &nodeStateCollector{
		node: node,
		roleDesc: prometheus.NewDesc(
			"raft_role",
			"Current raft role as one-hot gauge.",
			[]string{"role"},
			constLabels,
		),
		termDesc: prometheus.NewDesc(
			"raft_term",
			"Current raft term.",
			nil,
			constLabels,
		),
		logsDesc: prometheus.NewDesc(
			"raft_log_entries",
			"Current number of raft log entries stored on this node.",
			nil,
			constLabels,
		),
	}
}

func (c *nodeStateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.roleDesc
	ch <- c.termDesc
	ch <- c.logsDesc
}

func (c *nodeStateCollector) Collect(ch chan<- prometheus.Metric) {
	currentState := c.node.State()
	for _, role := range []raft.State{raft.Follower, raft.Candidate, raft.Leader} {
		value := 0.0
		if currentState == role {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(c.roleDesc, prometheus.GaugeValue, value, role.String())
	}
	ch <- prometheus.MustNewConstMetric(c.termDesc, prometheus.GaugeValue, float64(c.node.CurrentTerm()))
	ch <- prometheus.MustNewConstMetric(c.logsDesc, prometheus.GaugeValue, float64(c.node.StateMachine.Len()))
}
