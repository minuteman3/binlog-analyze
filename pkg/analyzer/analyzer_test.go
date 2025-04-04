package analyzer

import (
	"strings"
	"testing"
	"time"

	"github.com/miles/binlog-analyze/pkg/models"
)

func createTestStats() *models.BinlogStats {
	stats := models.NewBinlogStats("test-binlog.001")
	
	// Set overall time range
	stats.StartTime = "2023-01-01T10:00:00Z"
	stats.EndTime = "2023-01-01T11:00:00Z"
	
	// Add some table stats
	tableStats := stats.GetOrCreateTableStats("users")
	tableStats.Inserts = 100
	tableStats.Updates = 50
	tableStats.Deletes = 25
	tableStats.TotalDML = 175
	
	tableStats = stats.GetOrCreateTableStats("orders")
	tableStats.Inserts = 200
	tableStats.Updates = 30
	tableStats.Deletes = 10
	tableStats.TotalDML = 240
	
	// Add some transactions
	txn1 := &models.TransactionStats{
		XID:            1001,
		StartTime:      time.Date(2023, 1, 1, 10, 15, 0, 0, time.UTC),
		EndTime:        time.Date(2023, 1, 1, 10, 15, 5, 0, time.UTC),
		Duration:       5 * time.Second,
		TablesAffected: map[string]bool{"users": true},
		RowsChanged:    50,
		ByteSize:       5 * 1024,
	}
	
	txn2 := &models.TransactionStats{
		XID:            1002,
		StartTime:      time.Date(2023, 1, 1, 10, 30, 0, 0, time.UTC),
		EndTime:        time.Date(2023, 1, 1, 10, 30, 20, 0, time.UTC),
		Duration:       20 * time.Second,
		TablesAffected: map[string]bool{"orders": true, "users": true},
		RowsChanged:    100,
		ByteSize:       10 * 1024,
	}
	
	txn3 := &models.TransactionStats{
		XID:            1003,
		StartTime:      time.Date(2023, 1, 1, 10, 45, 0, 0, time.UTC),
		EndTime:        time.Date(2023, 1, 1, 10, 45, 1, 0, time.UTC),
		Duration:       1 * time.Second,
		TablesAffected: map[string]bool{"orders": true},
		RowsChanged:    10,
		ByteSize:       1 * 1024,
	}
	
	stats.Transactions = append(stats.Transactions, txn1, txn2, txn3)
	stats.TotalEvents = 500
	stats.TotalRowsChanged = 160
	stats.TotalBytes = 16 * 1024
	
	return stats
}

func TestGetMarkdownReport(t *testing.T) {
	stats := createTestStats()
	analyzer := NewAnalyzer(stats)
	
	// Set custom top count to test the configuration
	analyzer.SetTopTransactionCount(5)
	
	report := analyzer.GetMarkdownReport()
	
	// Check that the report contains expected time range
	if !strings.Contains(report, "2023-01-01 10:00:00 to 2023-01-01 11:00:00") {
		t.Errorf("Report should contain the time range")
	}
	
	// Check that transaction times are included
	if !strings.Contains(report, "| 1001 | 2023-01-01 10:15:00 | 2023-01-01 10:15:05 |") {
		t.Errorf("Report should contain transaction start and end times")
	}
	
	// Check that the custom top count is used
	if !strings.Contains(report, "### Top 5 longest transactions") {
		t.Errorf("Report should use custom top transaction count")
	}
	
	// Check that all tables are included
	if !strings.Contains(report, "| users ") || !strings.Contains(report, "| orders ") {
		t.Errorf("Report should contain all table stats")
	}
	
	// Check that transaction duration is correct
	if !strings.Contains(report, "5s") {
		t.Errorf("Report should contain correct transaction duration")
	}
	
	// Test with minimum duration filter
	analyzer.SetMinTransactionDuration(10 * time.Second)
	filteredReport := analyzer.GetMarkdownReport()
	
	// Should not include the 5s and 1s transactions
	if strings.Contains(filteredReport, "| 1001 |") || strings.Contains(filteredReport, "| 1003 |") {
		t.Errorf("Filtered report should not contain transactions shorter than 10s")
	}
	
	// Should include the 20s transaction
	if !strings.Contains(filteredReport, "| 1002 |") {
		t.Errorf("Filtered report should contain transactions longer than 10s")
	}
}

func TestPrintSummary(t *testing.T) {
	// This is a simple test to ensure PrintSummary doesn't crash
	stats := createTestStats()
	analyzer := NewAnalyzer(stats)
	
	// Just call the function to make sure it doesn't panic
	analyzer.PrintSummary()
}

func TestFormatByteSize(t *testing.T) {
	testCases := []struct {
		bytes    int64
		expected string
	}{
		{500, "500 bytes"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1.5, "1.50 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1024 * 1024 * 1024 * 2.5, "2.50 GB"},
	}
	
	for _, tc := range testCases {
		result := formatByteSize(tc.bytes)
		if result != tc.expected {
			t.Errorf("formatByteSize(%d) = %s, expected %s", tc.bytes, result, tc.expected)
		}
	}
}