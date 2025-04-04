package parser

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/miles/binlog-analyze/pkg/models"
)

// Parser handles the parsing of MySQL binlog files
type Parser struct {
	stats *models.BinlogStats
}

// NewParser creates a new parser for a specific binlog file
func NewParser(binlogFile string) *Parser {
	return &Parser{
		stats: models.NewBinlogStats(binlogFile),
	}
}

// ParseBinlog parses a MySQL binlog file and collects statistics
func (p *Parser) ParseBinlog() (*models.BinlogStats, error) {
	// Create a binlog parser
	parser := replication.NewBinlogParser()

	// Parse the binlog file with our event handler
	err := parser.ParseFile(p.stats.BinlogFile, 0, p.handleEvent)
	if err != nil {
		return nil, fmt.Errorf("error parsing binlog file: %w", err)
	}

	return p.stats, nil
}

// handleEvent processes each binlog event
func (p *Parser) handleEvent(e *replication.BinlogEvent) error {
	p.stats.TotalEvents++

	// Calculate event size in bytes (EventSize is the size of the event in the binlog)
	eventSize := int64(e.Header.EventSize)

	// Handle different event types
	switch event := e.Event.(type) {
	case *replication.RowsEvent:
		tableName := string(event.Table.Table)

		// Start transaction if this is the first operation (if not already started)
		p.stats.StartTransaction(e.Header.Timestamp)

		// Track event size for the current transaction
		p.stats.AddEventBytes(eventSize)

		// Check the event type to determine if it's an insert, update, or delete
		switch e.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			p.stats.IncrementInserts(tableName, len(event.Rows))
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			p.stats.IncrementUpdates(tableName, len(event.Rows)/2) // Updates have before/after pairs
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			p.stats.IncrementDeletes(tableName, len(event.Rows))
		}

	case *replication.QueryEvent:
		// Process SQL query events
		query := string(event.Query)

		// Start transaction for statements like "BEGIN"
		if query == "BEGIN" {
			p.stats.StartTransaction(e.Header.Timestamp)
		}

		// Track event size
		p.stats.AddEventBytes(eventSize)
		// Could process DDL statements here if needed

	case *replication.XIDEvent:
		// Transaction commit - XID is the unique transaction ID
		// Track final event size before commit
		p.stats.AddEventBytes(eventSize)
		p.stats.CommitTransaction(event.XID, e.Header.Timestamp)

	case *replication.FormatDescriptionEvent:
		// First event in the binlog, contains the binlog version and server info
		p.stats.StartTime = time.Unix(int64(e.Header.Timestamp), 0).Format(time.RFC3339)

	}

	// Update end time with each event
	p.stats.EndTime = time.Unix(int64(e.Header.Timestamp), 0).Format(time.RFC3339)

	return nil
}
