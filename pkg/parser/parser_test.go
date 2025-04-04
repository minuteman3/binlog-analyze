package parser

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// Mock BinlogEvent for testing
func createMockEvent(eventType replication.EventType, timestamp uint32) *replication.BinlogEvent {
	header := &replication.EventHeader{
		Timestamp: timestamp,
		EventType: eventType,
		EventSize: 100,
	}
	
	// Return a generic event - the actual event data will be set by the test
	return &replication.BinlogEvent{
		Header: header,
	}
}

// Test that we correctly handle transaction start and commit events
func TestTransactionHandling(t *testing.T) {
	p := NewParser("test-binlog.001")
	
	// Create timestamps for our test events
	startTime := uint32(1617235200) // 2021-04-01 00:00:00 UTC
	midTime := uint32(1617235205)   // 2021-04-01 00:00:05 UTC
	endTime := uint32(1617235210)   // 2021-04-01 00:00:10 UTC
	
	// Create a BEGIN query event to start a transaction
	beginEvent := createMockEvent(replication.QUERY_EVENT, startTime)
	beginEvent.Event = &replication.QueryEvent{
		Query: []byte("BEGIN"),
	}
	
	// Create a row event in the middle of the transaction
	rowEvent := createMockEvent(replication.WRITE_ROWS_EVENTv2, midTime)
	rowEvent.Event = &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Table: []byte("test_table"),
		},
		// Use []interface{} instead of []byte for Rows
		Rows: [][]interface{}{
			{interface{}("row1")},
			{interface{}("row2")},
		},
	}
	
	// Create a XID event to commit the transaction
	xidEvent := createMockEvent(replication.XID_EVENT, endTime)
	xidEvent.Event = &replication.XIDEvent{
		XID: 12345,
	}
	
	// Process the events
	err := p.handleEvent(beginEvent)
	if err != nil {
		t.Fatalf("Error handling BEGIN event: %v", err)
	}
	
	// Since currentTxn is unexported, we'll check the effects of the operations
	// rather than accessing it directly
	
	// Process a row event
	err = p.handleEvent(rowEvent)
	if err != nil {
		t.Fatalf("Error handling row event: %v", err)
	}
	
	// Check table stats were updated
	if tableStats, ok := p.stats.TableStats["test_table"]; !ok || tableStats.Inserts != 2 {
		var inserts int
		if tableStats != nil {
			inserts = tableStats.Inserts
		}
		t.Errorf("Expected 2 inserts for test_table, got %d", inserts)
	}
	
	// Process the XID (commit) event
	err = p.handleEvent(xidEvent)
	if err != nil {
		t.Fatalf("Error handling XID event: %v", err)
	}
	
	// Check that transaction was committed and added to the list
	if len(p.stats.Transactions) != 1 {
		t.Fatalf("Expected 1 transaction, got %d", len(p.stats.Transactions))
	}
	
	// Check the committed transaction properties
	txn := p.stats.Transactions[0]
	
	if txn.XID != 12345 {
		t.Errorf("Expected XID 12345, got %d", txn.XID)
	}
	
	expectedStartTime := time.Unix(int64(startTime), 0)
	if !txn.StartTime.Equal(expectedStartTime) {
		t.Errorf("Expected start time %v, got %v", expectedStartTime, txn.StartTime)
	}
	
	expectedEndTime := time.Unix(int64(endTime), 0)
	if !txn.EndTime.Equal(expectedEndTime) {
		t.Errorf("Expected end time %v, got %v", expectedEndTime, txn.EndTime)
	}
	
	expectedDuration := expectedEndTime.Sub(expectedStartTime)
	if txn.Duration != expectedDuration {
		t.Errorf("Expected duration %v, got %v", expectedDuration, txn.Duration)
	}
	
	// Check the byte size
	if txn.ByteSize != 300 { // 3 events * 100 bytes each
		t.Errorf("Expected 300 bytes, got %d", txn.ByteSize)
	}
	
	// Check that test_table was marked as affected
	if !txn.TablesAffected["test_table"] {
		t.Errorf("Expected test_table to be marked as affected")
	}
}

// Test handling of format description event (first event)
func TestFormatDescriptionEvent(t *testing.T) {
	p := NewParser("test-binlog.001")
	
	timestamp := uint32(1617235200) // 2021-04-01 00:00:00 UTC
	
	// Create a format description event
	formatEvent := createMockEvent(replication.FORMAT_DESCRIPTION_EVENT, timestamp)
	formatEvent.Event = &replication.FormatDescriptionEvent{}
	
	// Process the event
	err := p.handleEvent(formatEvent)
	if err != nil {
		t.Fatalf("Error handling format description event: %v", err)
	}
	
	// Check that the start time was set
	expectedTime := time.Unix(int64(timestamp), 0).Format(time.RFC3339)
	if p.stats.StartTime != expectedTime {
		t.Errorf("Expected start time %s, got %s", expectedTime, p.stats.StartTime)
	}
	
	// End time should also be set to the same value
	if p.stats.EndTime != expectedTime {
		t.Errorf("Expected end time %s, got %s", expectedTime, p.stats.EndTime)
	}
}

// Test handling of isolated row events (outside of a transaction)
func TestRowEvents(t *testing.T) {
	p := NewParser("test-binlog.001")
	
	timestamp := uint32(1617235200) // 2021-04-01 00:00:00 UTC
	
	// Create insert event
	insertEvent := createMockEvent(replication.WRITE_ROWS_EVENTv2, timestamp)
	insertEvent.Event = &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Table: []byte("users"),
		},
		Rows: [][]interface{}{
			{interface{}("row1")}, 
			{interface{}("row2")},
		},
	}
	
	// Process the event
	err := p.handleEvent(insertEvent)
	if err != nil {
		t.Fatalf("Error handling insert event: %v", err)
	}
	
	// Check that stats were updated
	if p.stats.TableStats["users"].Inserts != 2 {
		t.Errorf("Expected 2 inserts for users table, got %d", p.stats.TableStats["users"].Inserts)
	}
	
	// Create update event
	updateEvent := createMockEvent(replication.UPDATE_ROWS_EVENTv2, timestamp)
	updateEvent.Event = &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Table: []byte("orders"),
		},
		// Updates have before/after pairs
		Rows: [][]interface{}{
			{interface{}("before1")}, 
			{interface{}("after1")}, 
			{interface{}("before2")}, 
			{interface{}("after2")},
		},
	}
	
	// Process the event
	err = p.handleEvent(updateEvent)
	if err != nil {
		t.Fatalf("Error handling update event: %v", err)
	}
	
	// Check that stats were updated (updates count as pairs)
	if p.stats.TableStats["orders"].Updates != 2 {
		t.Errorf("Expected 2 updates for orders table, got %d", p.stats.TableStats["orders"].Updates)
	}
	
	// Create delete event
	deleteEvent := createMockEvent(replication.DELETE_ROWS_EVENTv2, timestamp)
	deleteEvent.Event = &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Table: []byte("products"),
		},
		Rows: [][]interface{}{{interface{}("row1")}},
	}
	
	// Process the event
	err = p.handleEvent(deleteEvent)
	if err != nil {
		t.Fatalf("Error handling delete event: %v", err)
	}
	
	// Check that stats were updated
	if p.stats.TableStats["products"].Deletes != 1 {
		t.Errorf("Expected 1 delete for products table, got %d", p.stats.TableStats["products"].Deletes)
	}
	
	// Check that total rows changed was updated correctly
	if p.stats.TotalRowsChanged != 5 { // 2 inserts + 2 updates + 1 delete
		t.Errorf("Expected 5 total rows changed, got %d", p.stats.TotalRowsChanged)
	}
}