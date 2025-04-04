package models

import (
	"testing"
	"time"
)

func TestStartTransaction(t *testing.T) {
	stats := NewBinlogStats("test-binlog.001")
	timestamp := uint32(1617235200) // 2021-04-01 00:00:00 UTC

	stats.StartTransaction(timestamp)

	if stats.currentTxn == nil {
		t.Fatalf("StartTransaction should initialize currentTxn")
	}

	expectedTime := time.Unix(int64(timestamp), 0)
	if !stats.currentTxn.StartTime.Equal(expectedTime) {
		t.Errorf("Expected start time %v, got %v", expectedTime, stats.currentTxn.StartTime)
	}

	if stats.currentTxn.TablesAffected == nil {
		t.Errorf("TablesAffected map should be initialized")
	}
}

func TestCommitTransaction(t *testing.T) {
	stats := NewBinlogStats("test-binlog.001")
	startTimestamp := uint32(1617235200) // 2021-04-01 00:00:00 UTC
	endTimestamp := uint32(1617235210)   // 2021-04-01 00:00:10 UTC
	xid := uint64(12345)

	// Initialize a transaction
	stats.StartTransaction(startTimestamp)
	
	// Add some changes to make RowsChanged > 0
	stats.currentTxn.RowsChanged = 5
	stats.currentTxn.TablesAffected["test_table"] = true

	// Commit the transaction
	stats.CommitTransaction(xid, endTimestamp)

	// Make sure the transaction was added to the list
	if len(stats.Transactions) != 1 {
		t.Fatalf("Expected 1 transaction, got %d", len(stats.Transactions))
	}

	txn := stats.Transactions[0]

	// Check the transaction properties
	if txn.XID != xid {
		t.Errorf("Expected XID %d, got %d", xid, txn.XID)
	}

	expectedStartTime := time.Unix(int64(startTimestamp), 0)
	if !txn.StartTime.Equal(expectedStartTime) {
		t.Errorf("Expected start time %v, got %v", expectedStartTime, txn.StartTime)
	}

	expectedEndTime := time.Unix(int64(endTimestamp), 0)
	if !txn.EndTime.Equal(expectedEndTime) {
		t.Errorf("Expected end time %v, got %v", expectedEndTime, txn.EndTime)
	}

	expectedDuration := expectedEndTime.Sub(expectedStartTime)
	if txn.Duration != expectedDuration {
		t.Errorf("Expected duration %v, got %v", expectedDuration, txn.Duration)
	}

	// Check that the current transaction was reset
	if stats.currentTxn != nil {
		t.Errorf("currentTxn should be nil after commit")
	}

	// Test with empty transaction (should not be added)
	stats.StartTransaction(startTimestamp)
	stats.CommitTransaction(xid+1, endTimestamp)
	
	// The transaction count should still be 1
	if len(stats.Transactions) != 1 {
		t.Errorf("Expected 1 transaction, got %d", len(stats.Transactions))
	}
}

func TestIncrementOperations(t *testing.T) {
	stats := NewBinlogStats("test-binlog.001")
	timestamp := uint32(1617235200) // 2021-04-01 00:00:00 UTC
	
	// Start a transaction
	stats.StartTransaction(timestamp)
	
	// Test insert increments
	stats.IncrementInserts("table1", 5)
	
	// Check table stats
	if stats.TableStats["table1"].Inserts != 5 {
		t.Errorf("Expected 5 inserts for table1, got %d", stats.TableStats["table1"].Inserts)
	}
	if stats.TableStats["table1"].TotalDML != 5 {
		t.Errorf("Expected 5 total DML for table1, got %d", stats.TableStats["table1"].TotalDML)
	}
	
	// Check transaction tracking
	if stats.currentTxn.RowsChanged != 5 {
		t.Errorf("Expected 5 rows changed in transaction, got %d", stats.currentTxn.RowsChanged)
	}
	if !stats.currentTxn.TablesAffected["table1"] {
		t.Errorf("table1 should be marked as affected in transaction")
	}
	
	// Test update increments
	stats.IncrementUpdates("table2", 3)
	
	// Check table stats
	if stats.TableStats["table2"].Updates != 3 {
		t.Errorf("Expected 3 updates for table2, got %d", stats.TableStats["table2"].Updates)
	}
	if stats.TableStats["table2"].TotalDML != 3 {
		t.Errorf("Expected 3 total DML for table2, got %d", stats.TableStats["table2"].TotalDML)
	}
	
	// Check transaction tracking
	if stats.currentTxn.RowsChanged != 8 { // 5 + 3
		t.Errorf("Expected 8 rows changed in transaction, got %d", stats.currentTxn.RowsChanged)
	}
	if !stats.currentTxn.TablesAffected["table2"] {
		t.Errorf("table2 should be marked as affected in transaction")
	}
	
	// Test delete increments
	stats.IncrementDeletes("table3", 2)
	
	// Check table stats
	if stats.TableStats["table3"].Deletes != 2 {
		t.Errorf("Expected 2 deletes for table3, got %d", stats.TableStats["table3"].Deletes)
	}
	if stats.TableStats["table3"].TotalDML != 2 {
		t.Errorf("Expected 2 total DML for table3, got %d", stats.TableStats["table3"].TotalDML)
	}
	
	// Check transaction tracking
	if stats.currentTxn.RowsChanged != 10 { // 5 + 3 + 2
		t.Errorf("Expected 10 rows changed in transaction, got %d", stats.currentTxn.RowsChanged)
	}
	if !stats.currentTxn.TablesAffected["table3"] {
		t.Errorf("table3 should be marked as affected in transaction")
	}
	
	// Check total rows changed
	if stats.TotalRowsChanged != 10 {
		t.Errorf("Expected 10 total rows changed, got %d", stats.TotalRowsChanged)
	}
}

func TestAddEventBytes(t *testing.T) {
	stats := NewBinlogStats("test-binlog.001")
	timestamp := uint32(1617235200) // 2021-04-01 00:00:00 UTC
	
	// Start a transaction
	stats.StartTransaction(timestamp)
	
	// Add bytes
	stats.AddEventBytes(1024)
	
	// Check total bytes
	if stats.TotalBytes != 1024 {
		t.Errorf("Expected 1024 total bytes, got %d", stats.TotalBytes)
	}
	
	// Check transaction bytes
	if stats.currentTxn.ByteSize != 1024 {
		t.Errorf("Expected 1024 bytes in transaction, got %d", stats.currentTxn.ByteSize)
	}
	
	// Add more bytes
	stats.AddEventBytes(2048)
	
	// Check updated total bytes
	if stats.TotalBytes != 3072 { // 1024 + 2048
		t.Errorf("Expected 3072 total bytes, got %d", stats.TotalBytes)
	}
	
	// Check updated transaction bytes
	if stats.currentTxn.ByteSize != 3072 { // 1024 + 2048
		t.Errorf("Expected 3072 bytes in transaction, got %d", stats.currentTxn.ByteSize)
	}
}

func TestMergeWith(t *testing.T) {
	// Create two BinlogStats instances
	stats1 := NewBinlogStats("binlog1.001")
	stats2 := NewBinlogStats("binlog2.001")
	
	// Set time ranges
	stats1.StartTime = "2021-04-01T00:00:00Z"
	stats1.EndTime = "2021-04-01T01:00:00Z"
	
	stats2.StartTime = "2021-04-01T01:30:00Z"
	stats2.EndTime = "2021-04-01T02:30:00Z"
	
	// Add some table stats
	stats1.GetOrCreateTableStats("table1").Inserts = 10
	stats1.GetOrCreateTableStats("table1").Updates = 5
	stats1.GetOrCreateTableStats("table1").Deletes = 2
	stats1.GetOrCreateTableStats("table1").TotalDML = 17
	
	stats2.GetOrCreateTableStats("table1").Inserts = 5
	stats2.GetOrCreateTableStats("table1").Updates = 3
	stats2.GetOrCreateTableStats("table1").Deletes = 1
	stats2.GetOrCreateTableStats("table1").TotalDML = 9
	
	stats2.GetOrCreateTableStats("table2").Inserts = 20
	stats2.GetOrCreateTableStats("table2").Updates = 0
	stats2.GetOrCreateTableStats("table2").Deletes = 0
	stats2.GetOrCreateTableStats("table2").TotalDML = 20
	
	// Add some transactions
	txn1 := &TransactionStats{
		XID:            1,
		StartTime:      time.Date(2021, 4, 1, 0, 30, 0, 0, time.UTC),
		EndTime:        time.Date(2021, 4, 1, 0, 30, 10, 0, time.UTC),
		Duration:       10 * time.Second,
		TablesAffected: map[string]bool{"table1": true},
		RowsChanged:    5,
		ByteSize:       1024,
	}
	
	txn2 := &TransactionStats{
		XID:            2,
		StartTime:      time.Date(2021, 4, 1, 2, 0, 0, 0, time.UTC),
		EndTime:        time.Date(2021, 4, 1, 2, 0, 15, 0, time.UTC),
		Duration:       15 * time.Second,
		TablesAffected: map[string]bool{"table2": true},
		RowsChanged:    10,
		ByteSize:       2048,
	}
	
	stats1.Transactions = append(stats1.Transactions, txn1)
	stats2.Transactions = append(stats2.Transactions, txn2)
	
	// Set other counters
	stats1.TotalEvents = 100
	stats1.TotalRowsChanged = 17
	stats1.TotalBytes = 5000
	
	stats2.TotalEvents = 150
	stats2.TotalRowsChanged = 29
	stats2.TotalBytes = 8000
	
	// Merge stats2 into stats1
	stats1.MergeWith(stats2)
	
	// Check merged results
	
	// Check time range
	if stats1.StartTime != "2021-04-01T00:00:00Z" {
		t.Errorf("Expected start time 2021-04-01T00:00:00Z, got %s", stats1.StartTime)
	}
	
	if stats1.EndTime != "2021-04-01T02:30:00Z" {
		t.Errorf("Expected end time 2021-04-01T02:30:00Z, got %s", stats1.EndTime)
	}
	
	// Check table stats
	if stats1.TableStats["table1"].Inserts != 15 { // 10 + 5
		t.Errorf("Expected 15 inserts for table1, got %d", stats1.TableStats["table1"].Inserts)
	}
	
	if stats1.TableStats["table1"].Updates != 8 { // 5 + 3
		t.Errorf("Expected 8 updates for table1, got %d", stats1.TableStats["table1"].Updates)
	}
	
	if stats1.TableStats["table1"].Deletes != 3 { // 2 + 1
		t.Errorf("Expected 3 deletes for table1, got %d", stats1.TableStats["table1"].Deletes)
	}
	
	if stats1.TableStats["table1"].TotalDML != 26 { // 17 + 9
		t.Errorf("Expected 26 total DML for table1, got %d", stats1.TableStats["table1"].TotalDML)
	}
	
	if stats1.TableStats["table2"].Inserts != 20 {
		t.Errorf("Expected 20 inserts for table2, got %d", stats1.TableStats["table2"].Inserts)
	}
	
	// Check transactions
	if len(stats1.Transactions) != 2 {
		t.Errorf("Expected 2 transactions, got %d", len(stats1.Transactions))
	}
	
	// Check counters
	if stats1.TotalEvents != 250 { // 100 + 150
		t.Errorf("Expected 250 total events, got %d", stats1.TotalEvents)
	}
	
	if stats1.TotalRowsChanged != 46 { // 17 + 29
		t.Errorf("Expected 46 total rows changed, got %d", stats1.TotalRowsChanged)
	}
	
	if stats1.TotalBytes != 13000 { // 5000 + 8000
		t.Errorf("Expected 13000 total bytes, got %d", stats1.TotalBytes)
	}
	
	// Check binlog file info
	if stats1.BinlogFile != "Multiple files: binlog1.001, binlog2.001" {
		t.Errorf("Expected binlog file 'Multiple files: binlog1.001, binlog2.001', got '%s'", stats1.BinlogFile)
	}
}