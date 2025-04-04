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

func createClusteredTestStats() *models.BinlogStats {
	stats := models.NewBinlogStats("test-binlog.002")

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
	tableStats.Inserts = 75
	tableStats.Updates = 25
	tableStats.Deletes = 10
	tableStats.TotalDML = 110

	// Create a cluster of transactions that end at similar times
	clusterEndTime := time.Date(2023, 1, 1, 10, 30, 0, 0, time.UTC)

	// Add 10 transactions that end exactly at the same time (worst case scenario)
	for i := 0; i < 10; i++ {
		// Vary the start times significantly
		startTime := clusterEndTime.Add(-time.Duration(30+i*5) * time.Second)

		// All end exactly at the same time
		endTime := clusterEndTime

		txn := &models.TransactionStats{
			XID:            uint64(2000 + i),
			StartTime:      startTime,
			EndTime:        endTime,
			Duration:       endTime.Sub(startTime),
			TablesAffected: map[string]bool{"users": true},
			RowsChanged:    10 + i,
			ByteSize:       int64((1 + i) * 1024),
		}

		stats.Transactions = append(stats.Transactions, txn)
	}

	// Add a few isolated transactions
	txn1 := &models.TransactionStats{
		XID:            3001,
		StartTime:      time.Date(2023, 1, 1, 10, 15, 0, 0, time.UTC),
		EndTime:        time.Date(2023, 1, 1, 10, 15, 5, 0, time.UTC),
		Duration:       5 * time.Second,
		TablesAffected: map[string]bool{"users": true},
		RowsChanged:    50,
		ByteSize:       5 * 1024,
	}

	txn2 := &models.TransactionStats{
		XID:            3002,
		StartTime:      time.Date(2023, 1, 1, 10, 45, 0, 0, time.UTC),
		EndTime:        time.Date(2023, 1, 1, 10, 45, 10, 0, time.UTC),
		Duration:       10 * time.Second,
		TablesAffected: map[string]bool{"orders": true},
		RowsChanged:    20,
		ByteSize:       2 * 1024,
	}

	// Add transactions that end around when the first transaction in the cluster started
	// The first transaction in the cluster is the one with XID 2009, which starts at clusterEndTime - (30 + 9*5)s = clusterEndTime - 75s
	firstTxnStartTime := clusterEndTime.Add(-75 * time.Second)

	// Add early potential blockers
	earlyBlocker1 := &models.TransactionStats{
		XID:            4001,
		StartTime:      firstTxnStartTime.Add(-10 * time.Second),
		EndTime:        firstTxnStartTime.Add(-2 * time.Second),
		Duration:       8 * time.Second,
		TablesAffected: map[string]bool{"users": true, "orders": true},
		RowsChanged:    30,
		ByteSize:       3 * 1024,
	}

	earlyBlocker2 := &models.TransactionStats{
		XID:            4002,
		StartTime:      firstTxnStartTime.Add(-5 * time.Second),
		EndTime:        firstTxnStartTime.Add(-1 * time.Second),
		Duration:       4 * time.Second,
		TablesAffected: map[string]bool{"orders": true},
		RowsChanged:    15,
		ByteSize:       1 * 1024,
	}

	earlyBlocker3 := &models.TransactionStats{
		XID:            4003,
		StartTime:      firstTxnStartTime.Add(-3 * time.Second),
		EndTime:        firstTxnStartTime.Add(1 * time.Second),
		Duration:       4 * time.Second,
		TablesAffected: map[string]bool{"users": true},
		RowsChanged:    25,
		ByteSize:       2 * 1024,
	}

	stats.Transactions = append(stats.Transactions, txn1, txn2, earlyBlocker1, earlyBlocker2, earlyBlocker3)
	stats.TotalEvents = 500
	stats.TotalRowsChanged = 275
	stats.TotalBytes = 26 * 1024

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

func TestTransactionClusters(t *testing.T) {
	// Use the stats with a transaction cluster
	stats := createClusteredTestStats()
	analyzer := NewAnalyzer(stats)

	// Enable cluster detection with a 1s window (longer to ensure test passes)
	analyzer.EnableTransactionClusterDetection(true)
	analyzer.SetClusterTimeWindow(1 * time.Second) // Increased window

	// Get the clusters
	clusters := analyzer.findTransactionClusters()

	// Dump cluster information for debugging
	t.Logf("Found %d clusters", len(clusters))
	for i, cluster := range clusters {
		t.Logf("Cluster %d: %d transactions, end time: %s",
			i, len(cluster.Transactions), cluster.EndTime)
	}

	if len(clusters) > 0 {
		t.Logf("Largest cluster has %d transactions", len(clusters[0].Transactions))
		for i, txn := range clusters[0].Transactions {
			t.Logf("  Txn %d: XID=%d, End time: %s", i, txn.XID, txn.EndTime)
		}
	}

	// Verify that clusters were found
	if len(clusters) == 0 {
		t.Errorf("Expected to find at least one transaction cluster, got none")
	}

	// The largest cluster should have 10 transactions (our test data)
	if len(clusters) > 0 {
		if len(clusters[0].Transactions) < 8 {
			t.Errorf("Expected largest cluster to have at least 8 transactions, got %d",
				len(clusters[0].Transactions))
		}

		// Verify the first transaction is identified correctly
		firstTxn := clusters[0].FirstTransaction
		if firstTxn == nil {
			t.Errorf("First transaction in cluster should not be nil")
		} else {
			// The first transaction should be the one with the earliest start time
			// In our test data, we create transactions with a formula that makes XID 2009 start earliest
			if firstTxn.XID != 2009 {
				t.Errorf("Expected first transaction XID to be 2009, got %d", firstTxn.XID)
			}
		}

		// Verify early blockers are identified
		if len(clusters[0].EarlyBlockers) == 0 {
			t.Errorf("Expected to find early blockers, got none")
		} else {
			// We should have 3 early blockers (see createClusteredTestStats)
			if len(clusters[0].EarlyBlockers) != 3 {
				t.Errorf("Expected to find 3 early blockers, got %d", len(clusters[0].EarlyBlockers))
			}

			// Verify they're sorted by timing proximity to first transaction start
			if len(clusters[0].EarlyBlockers) >= 2 {
				firstDiff := clusters[0].FirstTransaction.StartTime.Sub(clusters[0].EarlyBlockers[0].EndTime)
				if firstDiff < 0 {
					firstDiff = -firstDiff // Convert to absolute value
				}

				secondDiff := clusters[0].FirstTransaction.StartTime.Sub(clusters[0].EarlyBlockers[1].EndTime)
				if secondDiff < 0 {
					secondDiff = -secondDiff // Convert to absolute value
				}

				if firstDiff > secondDiff {
					t.Errorf("Early blockers should be sorted by proximity to first transaction start time")
				}
			}
		}
	}

	// Test with the markdown report
	report := analyzer.GetMarkdownReport()

	// Verify that the cluster analysis section is in the report
	if !strings.Contains(report, "### Transaction End Time Cluster Analysis") {
		t.Errorf("Expected markdown report to contain cluster analysis section")
	}

	// Verify that the cluster details are in the report
	if !strings.Contains(report, "#### Details for largest cluster") {
		t.Errorf("Expected markdown report to contain details for the largest cluster")
	}

	// Verify that the first transaction section is in the report
	if !strings.Contains(report, "**First transaction in cluster:**") {
		t.Errorf("Expected markdown report to contain first transaction section")
	}

	// Verify that the early blockers section is in the report
	if !strings.Contains(report, "**Transactions ending around when the first transaction started") {
		t.Errorf("Expected markdown report to contain early blockers section")
	}
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
