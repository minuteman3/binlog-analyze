package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miles/binlog-analyze/pkg/analyzer"
	"github.com/miles/binlog-analyze/pkg/models"
)

// TestWriteToFile tests the file writing functionality
func TestWriteToFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "binlog-analyze-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file path
	testFile := filepath.Join(tempDir, "test-output.txt")

	// Test writing to the file
	testContent := "Test content for the file."
	err = writeToFile(testFile, testContent)
	if err != nil {
		t.Fatalf("writeToFile failed: %v", err)
	}

	// Read back the file to verify content
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read test file: %v", err)
	}

	if string(content) != testContent {
		t.Errorf("Expected content %q, got %q", testContent, string(content))
	}
}

// TestIntegration tests the integration between models, analyzer, and output generation
func TestIntegration(t *testing.T) {
	// Create a test binlog stats object
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

	// Add a transaction with specific start and end times
	txn := &models.TransactionStats{
		XID:            1001,
		StartTime:      time.Date(2023, 1, 1, 10, 15, 0, 0, time.UTC),
		EndTime:        time.Date(2023, 1, 1, 10, 15, 5, 0, time.UTC),
		Duration:       5 * time.Second,
		TablesAffected: map[string]bool{"users": true},
		RowsChanged:    50,
		ByteSize:       5 * 1024,
	}

	stats.Transactions = append(stats.Transactions, txn)
	stats.TotalEvents = 100
	stats.TotalRowsChanged = 50
	stats.TotalBytes = 5 * 1024

	// Initialize analyzer
	a := analyzer.NewAnalyzer(stats)

	// Get markdown report
	report := a.GetMarkdownReport()

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "binlog-analyze-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Write report to a file
	testFile := filepath.Join(tempDir, "test-report.md")
	err = writeToFile(testFile, report)
	if err != nil {
		t.Fatalf("Failed to write report to file: %v", err)
	}

	// Verify file exists
	_, err = os.Stat(testFile)
	if err != nil {
		t.Fatalf("Failed to verify output file: %v", err)
	}

	// Read back the file and check if it contains expected content
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read test report file: %v", err)
	}

	// Check for key elements in the report
	reportContent := string(content)

	// Check for transaction times
	if !contains(reportContent, "2023-01-01 10:15:00") || !contains(reportContent, "2023-01-01 10:15:05") {
		t.Errorf("Report should contain transaction start and end times")
	}

	// Check for transaction duration
	if !contains(reportContent, "5s") {
		t.Errorf("Report should contain transaction duration")
	}

	// Check for table stats
	if !contains(reportContent, "users") {
		t.Errorf("Report should contain table stats")
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
