package analyzer

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/miles/binlog-analyze/pkg/models"
)

// Analyzer processes and formats binlog statistics
type Analyzer struct {
	stats                  *models.BinlogStats
	minTransactionDuration time.Duration
	topTransactionCount    int // Number of transactions to show in top lists
}

// NewAnalyzer creates a new analyzer with statistics
func NewAnalyzer(stats *models.BinlogStats) *Analyzer {
	return &Analyzer{
		stats:                  stats,
		minTransactionDuration: 0,
		topTransactionCount:    10, // Default to showing top 10 transactions
	}
}

// SetMinTransactionDuration sets the minimum duration filter for transactions
func (a *Analyzer) SetMinTransactionDuration(duration time.Duration) {
	a.minTransactionDuration = duration
}

// SetTopTransactionCount sets the number of transactions to show in top lists
func (a *Analyzer) SetTopTransactionCount(count int) {
	if count < 1 {
		count = 1 // Ensure at least 1 transaction is shown
	}
	a.topTransactionCount = count
}

// PrintSummary outputs a summary of the binlog statistics
func (a *Analyzer) PrintSummary() {
	fmt.Println("=== MySQL Binlog Analysis Summary ===")
	fmt.Printf("Binlog File: %s\n", a.stats.BinlogFile)
	
	start, _ := time.Parse(time.RFC3339, a.stats.StartTime)
	end, _ := time.Parse(time.RFC3339, a.stats.EndTime)
	duration := end.Sub(start)
	
	fmt.Printf("Time Range: %s to %s (Duration: %s)\n", 
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
		duration)
	
	fmt.Printf("Total Events: %d\n", a.stats.TotalEvents)
	fmt.Printf("Total Rows Changed: %d\n", a.stats.TotalRowsChanged)
	fmt.Printf("Total Bytes: %d (%s)\n", a.stats.TotalBytes, formatByteSize(a.stats.TotalBytes))
	fmt.Printf("Total Transactions: %d\n\n", len(a.stats.Transactions))
	
	a.printTableStats()
	a.printTransactionStats()
}

// printTableStats outputs statistics for each table
func (a *Analyzer) printTableStats() {
	fmt.Println("=== Table Statistics ===")
	
	// Get a sorted list of table names
	var tableNames []string
	for tableName := range a.stats.TableStats {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	
	// Print header
	fmt.Printf("%-30s | %-10s | %-10s | %-10s | %-10s\n", "Table", "Inserts", "Updates", "Deletes", "Total")
	fmt.Println(strings.Repeat("-", 80))
	
	// Print table stats
	for _, tableName := range tableNames {
		stats := a.stats.TableStats[tableName]
		fmt.Printf("%-30s | %-10d | %-10d | %-10d | %-10d\n",
			tableName,
			stats.Inserts,
			stats.Updates,
			stats.Deletes,
			stats.TotalDML)
	}
}

// Add a function to print transaction statistics
func (a *Analyzer) printTransactionStats() {
	// Only print if we have transactions
	if len(a.stats.Transactions) == 0 {
		return
	}

	// Filter transactions based on minimum duration
	var filteredTxns []*models.TransactionStats
	for _, txn := range a.stats.Transactions {
		if txn.Duration >= a.minTransactionDuration {
			filteredTxns = append(filteredTxns, txn)
		}
	}

	// Skip if no transactions meet the filter criteria
	if len(filteredTxns) == 0 {
		if a.minTransactionDuration > 0 {
			fmt.Printf("\nNo transactions found with duration >= %s\n", a.minTransactionDuration)
		}
		return
	}

	// --- TOP N BY DURATION ---
	// Sort transactions by duration (longest first)
	durationSorted := make([]*models.TransactionStats, len(filteredTxns))
	copy(durationSorted, filteredTxns)
	sort.Slice(durationSorted, func(i, j int) bool {
		return durationSorted[i].Duration > durationSorted[j].Duration
	})

	// Print section header
	fmt.Println("\n=== Transaction Statistics ===")
	if a.minTransactionDuration > 0 {
		fmt.Printf("Showing transactions with duration >= %s\n", a.minTransactionDuration)
	}
	
	fmt.Printf("\nTop %d longest transactions:\n", a.topTransactionCount)
	
	// Print header
	fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n", 
		"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
	fmt.Println(strings.Repeat("-", 160))
	
	// Limit to top N transactions based on configuration
	limit := a.topTransactionCount
	if len(durationSorted) < limit {
		limit = len(durationSorted)
	}

	// Print transaction stats
	for i := 0; i < limit; i++ {
		txn := durationSorted[i]
		
		// Get affected tables
		var tables []string
		for table := range txn.TablesAffected {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		
		fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-10d | %s\n",
			txn.XID,
			txn.StartTime.Format("2006-01-02 15:04:05"),
			txn.EndTime.Format("2006-01-02 15:04:05"),
			txn.Duration.String(),
			txn.RowsChanged,
			formatByteSize(txn.ByteSize),
			len(txn.TablesAffected),
			strings.Join(tables, ", "))
	}
	
	// --- TOP N BY ROWS TOUCHED ---
	// Sort transactions by rows changed (highest first)
	rowsSorted := make([]*models.TransactionStats, len(filteredTxns))
	copy(rowsSorted, filteredTxns)
	sort.Slice(rowsSorted, func(i, j int) bool {
		return rowsSorted[i].RowsChanged > rowsSorted[j].RowsChanged
	})
	
	fmt.Printf("\nTop %d transactions by rows changed:\n", a.topTransactionCount)
	
	// Print header
	fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n", 
		"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
	fmt.Println(strings.Repeat("-", 160))
	
	// Limit to top N transactions based on configuration
	limit = a.topTransactionCount
	if len(rowsSorted) < limit {
		limit = len(rowsSorted)
	}

	// Print transaction stats
	for i := 0; i < limit; i++ {
		txn := rowsSorted[i]
		
		// Get affected tables
		var tables []string
		for table := range txn.TablesAffected {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		
		fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-10d | %s\n",
			txn.XID,
			txn.StartTime.Format("2006-01-02 15:04:05"),
			txn.EndTime.Format("2006-01-02 15:04:05"),
			txn.Duration.String(),
			txn.RowsChanged,
			formatByteSize(txn.ByteSize),
			len(txn.TablesAffected),
			strings.Join(tables, ", "))
	}
	
	// --- TOP N BY BYTES WRITTEN ---
	// Sort transactions by byte size (largest first)
	bytesSorted := make([]*models.TransactionStats, len(filteredTxns))
	copy(bytesSorted, filteredTxns)
	sort.Slice(bytesSorted, func(i, j int) bool {
		return bytesSorted[i].ByteSize > bytesSorted[j].ByteSize
	})
	
	fmt.Printf("\nTop %d transactions by bytes written:\n", a.topTransactionCount)
	
	// Print header
	fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n", 
		"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
	fmt.Println(strings.Repeat("-", 160))
	
	// Limit to top N transactions based on configuration
	limit = a.topTransactionCount
	if len(bytesSorted) < limit {
		limit = len(bytesSorted)
	}

	// Print transaction stats
	for i := 0; i < limit; i++ {
		txn := bytesSorted[i]
		
		// Get affected tables
		var tables []string
		for table := range txn.TablesAffected {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		
		fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-10d | %s\n",
			txn.XID,
			txn.StartTime.Format("2006-01-02 15:04:05"),
			txn.EndTime.Format("2006-01-02 15:04:05"),
			txn.Duration.String(),
			txn.RowsChanged,
			formatByteSize(txn.ByteSize),
			len(txn.TablesAffected),
			strings.Join(tables, ", "))
	}
}

// GetMarkdownReport generates a markdown report of the binlog statistics
func (a *Analyzer) GetMarkdownReport() string {
	var sb strings.Builder
	
	sb.WriteString("# MySQL Binlog Analysis Report\n\n")
	sb.WriteString(fmt.Sprintf("**Binlog File:** %s\n\n", a.stats.BinlogFile))
	
	start, _ := time.Parse(time.RFC3339, a.stats.StartTime)
	end, _ := time.Parse(time.RFC3339, a.stats.EndTime)
	duration := end.Sub(start)
	
	sb.WriteString(fmt.Sprintf("**Time Range:** %s to %s (Duration: %s)\n\n", 
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
		duration))
	
	sb.WriteString(fmt.Sprintf("**Total Events:** %d\n\n", a.stats.TotalEvents))
	sb.WriteString(fmt.Sprintf("**Total Rows Changed:** %d\n\n", a.stats.TotalRowsChanged))
	sb.WriteString(fmt.Sprintf("**Total Bytes:** %d (%s)\n\n", a.stats.TotalBytes, formatByteSize(a.stats.TotalBytes)))
	sb.WriteString(fmt.Sprintf("**Total Transactions:** %d\n\n", len(a.stats.Transactions)))
	
	// Table statistics
	sb.WriteString("## Table Statistics\n\n")
	
	sb.WriteString("| Table | Inserts | Updates | Deletes | Total |\n")
	sb.WriteString("|-------|---------|---------|---------|-------|\n")
	
	// Get a sorted list of table names
	var tableNames []string
	for tableName := range a.stats.TableStats {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	
	// Add table stats
	for _, tableName := range tableNames {
		stats := a.stats.TableStats[tableName]
		sb.WriteString(fmt.Sprintf("| %s | %d | %d | %d | %d |\n",
			tableName,
			stats.Inserts,
			stats.Updates,
			stats.Deletes,
			stats.TotalDML))
	}
	
	// Transaction statistics
	if len(a.stats.Transactions) > 0 {
		// Filter transactions based on minimum duration
		var filteredTxns []*models.TransactionStats
		for _, txn := range a.stats.Transactions {
			if txn.Duration >= a.minTransactionDuration {
				filteredTxns = append(filteredTxns, txn)
			}
		}
		
		if len(filteredTxns) > 0 {
			sb.WriteString("\n## Transaction Statistics\n\n")
			
			if a.minTransactionDuration > 0 {
				sb.WriteString(fmt.Sprintf("Transactions with duration >= %s\n\n", a.minTransactionDuration))
			}
			
			// --- TOP N BY DURATION ---
			// Sort transactions by duration (longest first)
			durationSorted := make([]*models.TransactionStats, len(filteredTxns))
			copy(durationSorted, filteredTxns)
			sort.Slice(durationSorted, func(i, j int) bool {
				return durationSorted[i].Duration > durationSorted[j].Duration
			})
			
			sb.WriteString(fmt.Sprintf("### Top %d longest transactions\n\n", a.topTransactionCount))
			
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows Changed | Bytes | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|------------|----------|----------|--------------|-------|--------|----------------|\n")
			
			// Limit to top N transactions based on configuration
			limit := a.topTransactionCount
			if len(durationSorted) < limit {
				limit = len(durationSorted)
			}
			
			// Add transaction stats
			for i := 0; i < limit; i++ {
				txn := durationSorted[i]
				
				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)
				
				sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %d | %s |\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					len(txn.TablesAffected),
					strings.Join(tables, ", ")))
			}
			
			// --- TOP N BY ROWS TOUCHED ---
			// Sort transactions by rows changed (highest first)
			rowsSorted := make([]*models.TransactionStats, len(filteredTxns))
			copy(rowsSorted, filteredTxns)
			sort.Slice(rowsSorted, func(i, j int) bool {
				return rowsSorted[i].RowsChanged > rowsSorted[j].RowsChanged
			})
			
			sb.WriteString(fmt.Sprintf("\n### Top %d transactions by rows changed\n\n", a.topTransactionCount))
			
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows Changed | Bytes | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|------------|----------|----------|--------------|-------|--------|----------------|\n")
			
			// Limit to top N transactions based on configuration
			limit = a.topTransactionCount
			if len(rowsSorted) < limit {
				limit = len(rowsSorted)
			}
			
			// Add transaction stats
			for i := 0; i < limit; i++ {
				txn := rowsSorted[i]
				
				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)
				
				sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %d | %s |\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					len(txn.TablesAffected),
					strings.Join(tables, ", ")))
			}
			
			// --- TOP N BY BYTES WRITTEN ---
			// Sort transactions by byte size (largest first)
			bytesSorted := make([]*models.TransactionStats, len(filteredTxns))
			copy(bytesSorted, filteredTxns)
			sort.Slice(bytesSorted, func(i, j int) bool {
				return bytesSorted[i].ByteSize > bytesSorted[j].ByteSize
			})
			
			sb.WriteString(fmt.Sprintf("\n### Top %d transactions by bytes written\n\n", a.topTransactionCount))
			
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows Changed | Bytes | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|------------|----------|----------|--------------|-------|--------|----------------|\n")
			
			// Limit to top N transactions based on configuration
			limit = a.topTransactionCount
			if len(bytesSorted) < limit {
				limit = len(bytesSorted)
			}
			
			// Add transaction stats
			for i := 0; i < limit; i++ {
				txn := bytesSorted[i]
				
				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)
				
				sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %d | %s |\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					len(txn.TablesAffected),
					strings.Join(tables, ", ")))
			}
		} else if a.minTransactionDuration > 0 {
			sb.WriteString(fmt.Sprintf("\n## Transaction Statistics\n\nNo transactions found with duration >= %s\n", a.minTransactionDuration))
		}
	}
	
	return sb.String()
}

// formatByteSize formats bytes into a human-readable format (KB, MB, GB)
func formatByteSize(bytes int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	
	switch {
	case bytes >= gb:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}