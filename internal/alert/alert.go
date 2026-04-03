package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
)

// AlertLevel represents the severity of an alert
type AlertLevel string

const (
	AlertLevelInfo     AlertLevel = "info"
	AlertLevelWarning  AlertLevel = "warning"
	AlertLevelCritical AlertLevel = "critical"
	AlertLevelEmergency AlertLevel = "emergency"
)

// Alert represents an alert notification
type Alert struct {
	Level     AlertLevel  `json:"level"`
	Title     string      `json:"title"`
	Message   string      `json:"message"`
	Table     string      `json:"table,omitempty"`
	GapInfo   *cdc.GapInfo `json:"gap_info,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// AlertConfig contains alert configuration
type AlertConfig struct {
	Enabled   bool            `yaml:"enabled"`
	WebhookURL string         `yaml:"webhook_url,omitempty"`
	Channels  []AlertChannel `yaml:"channels"`
}

// AlertChannel represents a notification channel
type AlertChannel struct {
	Type       string   `yaml:"type"` // webhook, email, slack, feishu
	URL        string   `yaml:"url,omitempty"`
	Recipients []string `yaml:"recipients,omitempty"`
}

// AlertManager handles alert notifications
type AlertManager struct {
	config  AlertConfig
	client  *http.Client
}

// NewAlertManager creates a new alert manager
func NewAlertManager(config AlertConfig) *AlertManager {
	return &AlertManager{
		config: config,
		client: &http.Client{Timeout: 10 * time.Second},
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

// send dispatches the alert to configured channels
func (m *AlertManager) send(alert Alert) {
	if !m.config.Enabled {
		log.Printf("[ALERT %s] %s - %s (alerts disabled)", alert.Level, alert.Title, alert.Message)
		return
	}

	log.Printf("[ALERT %s] %s - %s", alert.Level, alert.Title, alert.Message)

	// Send to webhook if configured
	if m.config.WebhookURL != "" {
		if err := m.sendWebhook(m.config.WebhookURL, alert); err != nil {
			log.Printf("Failed to send webhook alert: %v", err)
		}
	}

	// Send to configured channels
	for _, channel := range m.config.Channels {
		switch channel.Type {
		case "webhook":
			if err := m.sendWebhook(channel.URL, alert); err != nil {
				log.Printf("Failed to send webhook alert: %v", err)
			}
		case "feishu":
			if err := m.sendFeishu(channel.URL, alert); err != nil {
				log.Printf("Failed to send Feishu alert: %v", err)
			}
		default:
			log.Printf("Unknown alert channel type: %s", channel.Type)
		}
	}
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
			log.Printf("resp.Body.Close error: %v", err)
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
					"tag":   "plain_text",
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
			log.Printf("resp.Body.Close error: %v", err)
		}
	}()

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

// Dummy context for unused parameter
var _ context.Context
