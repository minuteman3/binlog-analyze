package models

import (
	"strings"
	"time"
)

// TableStats holds statistics for a specific table
type TableStats struct {
	Inserts  int
	Updates  int
	Deletes  int
	TotalDML int
}

// TransactionStats holds statistics for a specific transaction
type TransactionStats struct {
	XID            uint64
	StartTime      time.Time
	EndTime        time.Time
	Duration       time.Duration
	TablesAffected map[string]bool
	RowsChanged    int
	ByteSize       int64 // Size of transaction in bytes
}

// BinlogStats holds aggregate statistics from binlog analysis
type BinlogStats struct {
	TableStats       map[string]*TableStats
	Transactions     []*TransactionStats
	TotalEvents      int
	TotalRowsChanged int
	TotalBytes       int64 // Total bytes across all events
	StartTime        string
	EndTime          string
	BinlogFile       string

	// Internal tracking for current transaction
	currentTxn       *TransactionStats
}

// NewBinlogStats creates a new BinlogStats instance
func NewBinlogStats(binlogFile string) *BinlogStats {
	return &BinlogStats{
		TableStats:   make(map[string]*TableStats),
		Transactions: make([]*TransactionStats, 0),
		BinlogFile:   binlogFile,
	}
}

// StartTransaction begins tracking a new transaction
func (bs *BinlogStats) StartTransaction(timestamp uint32) {
	if bs.currentTxn == nil {
		bs.currentTxn = &TransactionStats{
			StartTime:      time.Unix(int64(timestamp), 0),
			TablesAffected: make(map[string]bool),
		}
	}
}

// CommitTransaction finalizes the current transaction and adds it to the list
func (bs *BinlogStats) CommitTransaction(xid uint64, timestamp uint32) {
	if bs.currentTxn != nil {
		bs.currentTxn.XID = xid
		bs.currentTxn.EndTime = time.Unix(int64(timestamp), 0)
		bs.currentTxn.Duration = bs.currentTxn.EndTime.Sub(bs.currentTxn.StartTime)
		
		// Only add transactions that modified something
		if bs.currentTxn.RowsChanged > 0 {
			bs.Transactions = append(bs.Transactions, bs.currentTxn)
		}
		
		// Reset current transaction
		bs.currentTxn = nil
	}
}

// GetOrCreateTableStats returns existing table stats or creates a new one
func (bs *BinlogStats) GetOrCreateTableStats(tableName string) *TableStats {
	if stats, exists := bs.TableStats[tableName]; exists {
		return stats
	}
	
	bs.TableStats[tableName] = &TableStats{}
	return bs.TableStats[tableName]
}

// IncrementInserts increments the insert count for a table
func (bs *BinlogStats) IncrementInserts(tableName string, count int) {
	tableStats := bs.GetOrCreateTableStats(tableName)
	tableStats.Inserts += count
	tableStats.TotalDML += count
	bs.TotalRowsChanged += count
	
	// Track in current transaction if one exists
	if bs.currentTxn != nil {
		bs.currentTxn.TablesAffected[tableName] = true
		bs.currentTxn.RowsChanged += count
	}
}

// IncrementUpdates increments the update count for a table
func (bs *BinlogStats) IncrementUpdates(tableName string, count int) {
	tableStats := bs.GetOrCreateTableStats(tableName)
	tableStats.Updates += count
	tableStats.TotalDML += count
	bs.TotalRowsChanged += count
	
	// Track in current transaction if one exists
	if bs.currentTxn != nil {
		bs.currentTxn.TablesAffected[tableName] = true
		bs.currentTxn.RowsChanged += count
	}
}

// IncrementDeletes increments the delete count for a table
func (bs *BinlogStats) IncrementDeletes(tableName string, count int) {
	tableStats := bs.GetOrCreateTableStats(tableName)
	tableStats.Deletes += count
	tableStats.TotalDML += count
	bs.TotalRowsChanged += count
	
	// Track in current transaction if one exists
	if bs.currentTxn != nil {
		bs.currentTxn.TablesAffected[tableName] = true
		bs.currentTxn.RowsChanged += count
	}
}

// AddEventBytes adds event byte size to the current transaction and total
func (bs *BinlogStats) AddEventBytes(byteSize int64) {
	// Add to total bytes
	bs.TotalBytes += byteSize
	
	// Add to current transaction if one exists
	if bs.currentTxn != nil {
		bs.currentTxn.ByteSize += byteSize
	}
}

// MergeWith merges stats from another BinlogStats instance
func (bs *BinlogStats) MergeWith(other *BinlogStats) {
	// Merge basic counters
	bs.TotalEvents += other.TotalEvents
	bs.TotalRowsChanged += other.TotalRowsChanged
	bs.TotalBytes += other.TotalBytes
	
	// Merge table stats
	for tableName, otherTableStats := range other.TableStats {
		if tableStats, exists := bs.TableStats[tableName]; exists {
			// Update existing table stats
			tableStats.Inserts += otherTableStats.Inserts
			tableStats.Updates += otherTableStats.Updates
			tableStats.Deletes += otherTableStats.Deletes
			tableStats.TotalDML += otherTableStats.TotalDML
		} else {
			// Create new table stats
			bs.TableStats[tableName] = &TableStats{
				Inserts:  otherTableStats.Inserts,
				Updates:  otherTableStats.Updates,
				Deletes:  otherTableStats.Deletes,
				TotalDML: otherTableStats.TotalDML,
			}
		}
	}
	
	// Merge transactions (simply append them)
	bs.Transactions = append(bs.Transactions, other.Transactions...)
	
	// Update time range if needed
	otherStartTime, _ := time.Parse(time.RFC3339, other.StartTime)
	otherEndTime, _ := time.Parse(time.RFC3339, other.EndTime)
	thisStartTime, _ := time.Parse(time.RFC3339, bs.StartTime)
	thisEndTime, _ := time.Parse(time.RFC3339, bs.EndTime)
	
	// Set earliest start time
	if otherStartTime.Before(thisStartTime) || bs.StartTime == "" {
		bs.StartTime = other.StartTime
	}
	
	// Set latest end time
	if otherEndTime.After(thisEndTime) || bs.EndTime == "" {
		bs.EndTime = other.EndTime
	}
	
	// Update binlog file info to indicate it's a merged result
	if bs.BinlogFile != other.BinlogFile {
		if !strings.Contains(bs.BinlogFile, ", ") {
			// First merge - convert to a list format
			bs.BinlogFile = "Multiple files: " + bs.BinlogFile + ", " + other.BinlogFile
		} else {
			// Already a merged list - just append
			bs.BinlogFile = bs.BinlogFile + ", " + other.BinlogFile
		}
	}
}