package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all prometheus metrics for dbkrab
type Metrics struct {
	PollLagSeconds      prometheus.Gauge
	TransactionsTotal   prometheus.Counter
	TransactionsFailed  prometheus.Counter
	CDCGapBytes         prometheus.Gauge
	PollDurationSeconds prometheus.Histogram
	AlertsTotal         *prometheus.CounterVec
}

// New creates and registers all metrics
func New() *Metrics {
	return &Metrics{
		PollLagSeconds: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dbkrab_poll_lag_seconds",
			Help: "Current polling lag in seconds (maxLSN time - currentLSN time)",
		}),

		TransactionsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dbkrab_transactions_total",
			Help: "Total number of transactions processed",
		}),

		TransactionsFailed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dbkrab_transactions_failed_total",
			Help: "Total number of failed transactions",
		}),

		CDCGapBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dbkrab_cdc_gap_bytes",
			Help: "Current CDC gap in bytes (maxLSN - currentLSN)",
		}),

		PollDurationSeconds: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dbkrab_poll_duration_seconds",
			Help:    "Duration of each poll cycle in seconds",
			Buckets: prometheus.DefBuckets,
		}),

		AlertsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "dbkrab_alerts_total",
			Help: "Total number of alerts sent by level",
		}, []string{"level"}),
	}
}

// RecordPoll records metrics for a poll cycle
func (m *Metrics) RecordPoll(lagSeconds float64, gapBytes int64, durationSeconds float64) {
	m.PollLagSeconds.Set(lagSeconds)
	m.CDCGapBytes.Set(float64(gapBytes))
	m.PollDurationSeconds.Observe(durationSeconds)
}

// RecordTransaction records a processed transaction
func (m *Metrics) RecordTransaction(failed bool) {
	m.TransactionsTotal.Inc()
	if failed {
		m.TransactionsFailed.Inc()
	}
}

// RecordAlert records an alert sent
func (m *Metrics) RecordAlert(level string) {
	m.AlertsTotal.WithLabelValues(level).Inc()
}
