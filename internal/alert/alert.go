package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
)

// AlertLevel represents the severity of an alert
type AlertLevel string

const (
	AlertLevelInfo      AlertLevel = "info"
	AlertLevelWarning   AlertLevel = "warning"
	AlertLevelCritical  AlertLevel = "critical"
	AlertLevelEmergency AlertLevel = "emergency"
)

// Alert represents an alert notification
type Alert struct {
	Level     AlertLevel   `json:"level"`
	Title     string       `json:"title"`
	Message   string       `json:"message"`
	Table     string       `json:"table,omitempty"`
	GapInfo   *cdc.GapInfo `json:"gap_info,omitempty"`
	Timestamp time.Time    `json:"timestamp"`
}

// AlertConfig contains alert configuration
type AlertConfig struct {
	Enabled    bool           `yaml:"enabled"`
	WebhookURL string         `yaml:"webhook_url,omitempty"`
	Channels   []AlertChannel `yaml:"channels"`
}

// AlertChannel represents a notification channel
type AlertChannel struct {
	Type       string   `yaml:"type"` // webhook, email, slack, feishu
	URL        string   `yaml:"url,omitempty"`
	Recipients []string `yaml:"recipients,omitempty"`
}

// AlertManager handles alert notifications
type AlertManager struct {
	config AlertConfig
	client *http.Client
	wg     sync.WaitGroup
	sem    chan struct{} // Bounded concurrency for async alerts
}

// NewAlertManager creates a new alert manager
func NewAlertManager(config AlertConfig) *AlertManager {
	return &AlertManager{
		config: config,
		client: &http.Client{Timeout: 5 * time.Second}, // Shorter timeout to avoid blocking
		sem:    make(chan struct{}, 3),                 // Max 3 concurrent alert sends
	}
}

// SendWarning sends a warning-level alert
func (m *AlertManager) SendWarning(title string, gap cdc.GapInfo) {
	alert := Alert{
		Level:     AlertLevelWarning,
		Title:     title,
		Message:   fmt.Sprintf("CDC lag warning for %s: %d bytes, %v", gap.Table, gap.LagBytes, gap.LagDuration),
		Table:     gap.Table,
		GapInfo:   &gap,
		Timestamp: time.Now(),
	}
	m.send(alert)
}

// SendCritical sends a critical-level alert
func (m *AlertManager) SendCritical(title string, gap cdc.GapInfo) {
	alert := Alert{
		Level:     AlertLevelCritical,
		Title:     title,
		Message:   fmt.Sprintf("CDC lag critical for %s: %d bytes, %v", gap.Table, gap.LagBytes, gap.LagDuration),
		Table:     gap.Table,
		GapInfo:   &gap,
		Timestamp: time.Now(),
	}
	m.send(alert)
}

// SendEmergency sends an emergency-level alert (data loss detected)
func (m *AlertManager) SendEmergency(title string, gap cdc.GapInfo) {
	alert := Alert{
		Level:     AlertLevelEmergency,
		Title:     title,
		Message:   fmt.Sprintf("CDC DATA LOSS DETECTED for %s! Missing LSN range: %s - %s", gap.Table, formatLSN(gap.MissingLSNRange.Start), formatLSN(gap.MissingLSNRange.End)),
		Table:     gap.Table,
		GapInfo:   &gap,
		Timestamp: time.Now(),
	}
	m.send(alert)
}

// send dispatches the alert to configured channels asynchronously
// Uses bounded concurrency to avoid overwhelming the system
func (m *AlertManager) send(alert Alert) {
	if !m.config.Enabled {
		slog.Debug("alert skipped (disabled)", "level", alert.Level, "title", alert.Title)
		return
	}

	slog.Info("alert", "level", alert.Level, "title", alert.Title, "message", alert.Message)

	// Acquire semaphore (non-blocking for emergency alerts)
	if alert.Level == AlertLevelEmergency {
		// Emergency alerts block until they can send
		m.sem <- struct{}{}
		go func() {
			defer func() { <-m.sem }()
			m.sendSync(alert)
		}()
	} else {
		// Non-emergency: try to send, but don't block if too many in flight
		select {
		case m.sem <- struct{}{}:
			m.wg.Add(1)
			go func() {
				defer func() { <-m.sem; m.wg.Done() }()
				m.sendSync(alert)
			}()
		default:
			slog.Warn("dropping alert due to high load", "level", alert.Level, "title", alert.Title)
		}
	}
}

// sendSync sends alerts synchronously (called from goroutine)
func (m *AlertManager) sendSync(alert Alert) {
	// Send to webhook if configured
	if m.config.WebhookURL != "" {
		if err := m.sendWebhook(m.config.WebhookURL, alert); err != nil {
			slog.Error("failed to send webhook alert", "error", err)
		}
	}

	// Send to configured channels
	for _, channel := range m.config.Channels {
		switch channel.Type {
		case "webhook":
			if err := m.sendWebhook(channel.URL, alert); err != nil {
				slog.Error("failed to send webhook alert", "error", err)
			}
		case "feishu":
			if err := m.sendFeishu(channel.URL, alert); err != nil {
				slog.Error("failed to send Feishu alert", "error", err)
			}
		default:
			slog.Warn("unknown alert channel type", "type", channel.Type)
		}
	}
}

// Shutdown waits for all pending alerts to be sent
func (m *AlertManager) Shutdown() {
	m.wg.Wait()
}

// sendWebhook sends alert to a generic webhook
func (m *AlertManager) sendWebhook(url string, alert Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("marshal alert: %w", err)
	}

	resp, err := m.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("resp.Body.Close error", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// sendFeishu sends alert to Feishu webhook
func (m *AlertManager) sendFeishu(url string, alert Alert) error {
	// Feishu webhook format
	feishuMsg := map[string]interface{}{
		"msg_type": "interactive",
		"card": map[string]interface{}{
			"header": map[string]interface{}{
				"title": map[string]interface{}{
					"tag":     "plain_text",
					"content": fmt.Sprintf("[%s] %s", alert.Level, alert.Title),
				},
				"template": getFeishuTemplate(alert.Level),
			},
			"elements": []map[string]interface{}{
				{
					"tag": "div",
					"text": map[string]interface{}{
						"tag":     "lark_md",
						"content": alert.Message,
					},
				},
				{
					"tag": "div",
					"text": map[string]interface{}{
						"tag":     "lark_md",
						"content": fmt.Sprintf("**Table**: %s\n**Time**: %s", alert.Table, alert.Timestamp.Format(time.RFC3339)),
					},
				},
			},
		},
	}

	data, err := json.Marshal(feishuMsg)
	if err != nil {
		return fmt.Errorf("marshal feishu message: %w", err)
	}

	resp, err := m.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post feishu: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("resp.Body.Close error", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("feishu webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// getFeishuTemplate returns the card template color based on alert level
func getFeishuTemplate(level AlertLevel) string {
	switch level {
	case AlertLevelEmergency:
		return "red"
	case AlertLevelCritical:
		return "orange"
	case AlertLevelWarning:
		return "yellow"
	default:
		return "blue"
	}
}

// formatLSN formats an LSN value for display
func formatLSN(lsn []byte) string {
	if len(lsn) == 0 {
		return "<empty>"
	}
	return fmt.Sprintf("%X", lsn)
}
