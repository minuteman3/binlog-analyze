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
	stats                     *models.BinlogStats
	minTransactionDuration    time.Duration
	topTransactionCount       int           // Number of transactions to show in top lists
	detectTransactionClusters bool          // Whether to detect transaction end time clusters
	clusterTimeWindow         time.Duration // Time window to consider transactions as part of a cluster
}

// NewAnalyzer creates a new analyzer with statistics
func NewAnalyzer(stats *models.BinlogStats) *Analyzer {
	return &Analyzer{
		stats:                     stats,
		minTransactionDuration:    0,
		topTransactionCount:       10, // Default to showing top 10 transactions
		detectTransactionClusters: false,
		clusterTimeWindow:         time.Second, // Default to 1-second window for detecting clusters
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

// EnableTransactionClusterDetection enables detection of transaction end time clusters
func (a *Analyzer) EnableTransactionClusterDetection(enable bool) {
	a.detectTransactionClusters = enable
}

// SetClusterTimeWindow sets the time window for detecting transaction clusters
func (a *Analyzer) SetClusterTimeWindow(window time.Duration) {
	if window < time.Millisecond {
		window = time.Millisecond // Minimum 1ms window
	}
	a.clusterTimeWindow = window
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

	// Check for transaction clusters if enabled
	if a.detectTransactionClusters {
		a.printTransactionClusters()
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

			// Add transaction cluster analysis if enabled
			if a.detectTransactionClusters {
				a.addClusterAnalysisToMarkdown(&sb)
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

// TransactionCluster represents a group of transactions that end within a short time window
type TransactionCluster struct {
	EndTime                 time.Time
	Transactions            []*models.TransactionStats
	PotentialBlockers       []*models.TransactionStats // Transactions that might have blocked these ones
	ClusterStartTime        time.Time                  // When the first transaction in this cluster started getting "stuck"
	ApproximateBlockingTime time.Duration              // Approximate time the transactions were blocked
	FirstTransaction        *models.TransactionStats   // The first transaction in the cluster (earliest start time)
	EarlyBlockers           []*models.TransactionStats // Transactions that ended around the time the first transaction started
}

// findTransactionClusters identifies clusters of transactions that end around the same time
func (a *Analyzer) findTransactionClusters() []TransactionCluster {
	if len(a.stats.Transactions) == 0 || !a.detectTransactionClusters {
		return nil
	}

	// Filter transactions based on minimum duration if set
	var transactions []*models.TransactionStats
	for _, txn := range a.stats.Transactions {
		if txn.Duration >= a.minTransactionDuration {
			transactions = append(transactions, txn)
		}
	}

	if len(transactions) == 0 {
		return nil
	}

	// Sort transactions by end time
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].EndTime.Before(transactions[j].EndTime)
	})

	// Find clusters of transactions with similar end times
	// Since we're looking for transactions that end at the same time,
	// first group by exact end time to get an initial set of clusters
	endTimeMap := make(map[time.Time][]*models.TransactionStats)
	for _, txn := range transactions {
		endTimeMap[txn.EndTime] = append(endTimeMap[txn.EndTime], txn)
	}

	// Convert map to clusters
	var clusters []TransactionCluster
	for endTime, txns := range endTimeMap {
		if len(txns) > 1 {
			clusters = append(clusters, TransactionCluster{
				EndTime:      endTime,
				Transactions: txns,
			})
		}
	}

	// If a time window is specified, merge clusters that are close enough
	if a.clusterTimeWindow > 0 && len(clusters) > 1 {
		// Sort clusters by end time
		sort.Slice(clusters, func(i, j int) bool {
			return clusters[i].EndTime.Before(clusters[j].EndTime)
		})

		// Merge clusters that are close in time
		var mergedClusters []TransactionCluster
		currentCluster := clusters[0]

		for i := 1; i < len(clusters); i++ {
			timeDiff := clusters[i].EndTime.Sub(currentCluster.EndTime)
			if timeDiff < a.clusterTimeWindow {
				// Merge with current cluster
				currentCluster.Transactions = append(currentCluster.Transactions, clusters[i].Transactions...)

				// Update end time (weighted average)
				weightedTime1 := currentCluster.EndTime.UnixNano() * int64(len(currentCluster.Transactions)-len(clusters[i].Transactions))
				weightedTime2 := clusters[i].EndTime.UnixNano() * int64(len(clusters[i].Transactions))
				avgTime := (weightedTime1 + weightedTime2) / int64(len(currentCluster.Transactions))
				currentCluster.EndTime = time.Unix(0, avgTime)
			} else {
				// Add current cluster to result and start a new one
				mergedClusters = append(mergedClusters, currentCluster)
				currentCluster = clusters[i]
			}
		}

		// Add the last cluster
		mergedClusters = append(mergedClusters, currentCluster)
		clusters = mergedClusters
	}

	// Sort clusters by size (number of transactions), largest first
	sort.Slice(clusters, func(i, j int) bool {
		return len(clusters[i].Transactions) > len(clusters[j].Transactions)
	})

	// For each significant cluster (containing multiple transactions),
	// identify potential blocker transactions
	for i := range clusters {
		if len(clusters[i].Transactions) < 3 {
			// Only analyze significant clusters (3+ transactions)
			continue
		}

		cluster := &clusters[i]

		// Find the earliest start time among clustered transactions and identify the first transaction
		minStartTime := cluster.Transactions[0].StartTime
		cluster.FirstTransaction = cluster.Transactions[0]
		for _, txn := range cluster.Transactions {
			if txn.StartTime.Before(minStartTime) || (txn.StartTime.Equal(minStartTime) && txn.XID < cluster.FirstTransaction.XID) {
				minStartTime = txn.StartTime
				cluster.FirstTransaction = txn
			}
		}

		// Find the median start time, which is a better indicator of when
		// transactions started getting "stuck"
		startTimes := make([]time.Time, len(cluster.Transactions))
		for i, txn := range cluster.Transactions {
			startTimes[i] = txn.StartTime
		}

		// Sort start times
		sort.Slice(startTimes, func(i, j int) bool {
			return startTimes[i].Before(startTimes[j])
		})

		// Use the first quartile as a heuristic for when blocking began
		blockingStartIdx := len(startTimes) / 4
		if blockingStartIdx < 0 {
			blockingStartIdx = 0
		}
		blockingStartTime := startTimes[blockingStartIdx]

		// Store when the cluster started getting "stuck"
		cluster.ClusterStartTime = blockingStartTime

		// Calculate approximate blocking time (from blocking start to cluster end)
		cluster.ApproximateBlockingTime = cluster.EndTime.Sub(blockingStartTime)

		// Look for transactions that ended around the time the first transaction started
		var earlyBlockers []*models.TransactionStats
		firstTxnStartTime := cluster.FirstTransaction.StartTime

		// Look through all transactions to find early blockers
		for _, txn := range a.stats.Transactions {
			// Skip transactions that are part of this cluster
			isInCluster := false
			for _, clusterTxn := range cluster.Transactions {
				if txn.XID == clusterTxn.XID {
					isInCluster = true
					break
				}
			}
			if isInCluster {
				continue
			}

			// Check if this transaction ended around when the first transaction started
			// (within a window before and after the first transaction started)
			timeDiff := firstTxnStartTime.Sub(txn.EndTime)
			// Look for transactions that ended up to 5 seconds before or 2 seconds after the first transaction started
			if timeDiff > -2*time.Second && timeDiff < 5*time.Second {
				earlyBlockers = append(earlyBlockers, txn)
			}
		}

		// Sort early blockers by how close they ended to the first transaction start time
		sort.Slice(earlyBlockers, func(i, j int) bool {
			diffI := firstTxnStartTime.Sub(earlyBlockers[i].EndTime)
			if diffI < 0 {
				diffI = -diffI // Convert to absolute difference
			}
			diffJ := firstTxnStartTime.Sub(earlyBlockers[j].EndTime)
			if diffJ < 0 {
				diffJ = -diffJ // Convert to absolute difference
			}
			return diffI < diffJ // Sort by closest timing
		})

		// Store the early blockers
		cluster.EarlyBlockers = earlyBlockers

		// Now look for transactions that might have blocked this cluster
		// These are transactions that:
		// 1. Ended near when the blocking started
		// 2. Started before the blocking began
		var potentialBlockers []*models.TransactionStats

		// Look through all transactions to find potential blockers
		for _, txn := range a.stats.Transactions {
			// Skip transactions that are part of this cluster
			isInCluster := false
			for _, clusterTxn := range cluster.Transactions {
				if txn.XID == clusterTxn.XID {
					isInCluster = true
					break
				}
			}
			if isInCluster {
				continue
			}

			// Check if this transaction ended around when blocking started
			// (within a window before the blocking started)
			timeDiff := blockingStartTime.Sub(txn.EndTime)
			if timeDiff >= 0 && timeDiff < 10*time.Second {
				// This transaction ended shortly before the blocking started
				potentialBlockers = append(potentialBlockers, txn)
			}
		}

		// Sort potential blockers by end time (most recent first)
		sort.Slice(potentialBlockers, func(i, j int) bool {
			return potentialBlockers[i].EndTime.After(potentialBlockers[j].EndTime)
		})

		// Store the potential blockers
		cluster.PotentialBlockers = potentialBlockers
	}

	return clusters
}

// addClusterAnalysisToMarkdown adds transaction cluster analysis to a markdown report
func (a *Analyzer) addClusterAnalysisToMarkdown(sb *strings.Builder) {
	clusters := a.findTransactionClusters()
	if len(clusters) == 0 {
		return
	}

	sb.WriteString("### Transaction End Time Cluster Analysis\n\n")
	sb.WriteString(fmt.Sprintf("Detected %d clusters of transactions with similar end times (window: %s)\n\n",
		len(clusters), a.clusterTimeWindow))

	// Print the largest clusters (top 5 or less)
	limit := 5
	if len(clusters) < limit {
		limit = len(clusters)
	}

	sb.WriteString("#### Top clusters by transaction count (potential blocking events)\n\n")
	sb.WriteString("| Cluster End Time | Tx Count | Blocking Time | Avg Duration | Min Duration | Max Duration |\n")
	sb.WriteString("|------------------|----------|--------------|--------------|--------------|--------------|\n")

	// Also identify the longest cluster by average duration
	longestCluster := clusters[0]
	var maxAvgDuration time.Duration

	for i := 0; i < limit; i++ {
		cluster := clusters[i]

		// Calculate statistics about the cluster
		var totalDuration time.Duration
		minDuration := cluster.Transactions[0].Duration
		maxDuration := cluster.Transactions[0].Duration

		for _, txn := range cluster.Transactions {
			totalDuration += txn.Duration

			if txn.Duration < minDuration {
				minDuration = txn.Duration
			}
			if txn.Duration > maxDuration {
				maxDuration = txn.Duration
			}
		}

		avgDuration := totalDuration / time.Duration(len(cluster.Transactions))

		// Check if this cluster has a longer average duration
		if avgDuration > maxAvgDuration {
			maxAvgDuration = avgDuration
			longestCluster = cluster
		}

		blockingTime := "N/A"
		if !cluster.ClusterStartTime.IsZero() {
			blockingTime = cluster.ApproximateBlockingTime.String()
		}

		sb.WriteString(fmt.Sprintf("| %s | %d | %s | %s | %s | %s |\n",
			cluster.EndTime.Format("2006-01-02 15:04:05.000"),
			len(cluster.Transactions),
			blockingTime,
			avgDuration.String(),
			minDuration.String(),
			maxDuration.String()))
	}

	// For the largest cluster, show details about affected tables
	if len(clusters) > 0 {
		largestCluster := clusters[0]

		sb.WriteString(fmt.Sprintf("\n#### Details for largest cluster (%d transactions ending around %s)\n\n",
			len(largestCluster.Transactions),
			largestCluster.EndTime.Format("2006-01-02 15:04:05.000")))

		// Print information about the first transaction in the cluster
		if largestCluster.FirstTransaction != nil {
			sb.WriteString("**First transaction in cluster:**\n\n")
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------|----------------|\n")

			// Get affected tables
			var tables []string
			for table := range largestCluster.FirstTransaction.TablesAffected {
				tables = append(tables, table)
			}
			sort.Strings(tables)

			sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %d | %s |\n",
				largestCluster.FirstTransaction.XID,
				largestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05"),
				largestCluster.FirstTransaction.EndTime.Format("2006-01-02 15:04:05"),
				largestCluster.FirstTransaction.Duration.String(),
				largestCluster.FirstTransaction.RowsChanged,
				formatByteSize(largestCluster.FirstTransaction.ByteSize),
				len(largestCluster.FirstTransaction.TablesAffected),
				strings.Join(tables, ", ")))
		}

		// Display transactions that ended around when the first transaction started
		if largestCluster.FirstTransaction != nil && len(largestCluster.EarlyBlockers) > 0 {
			firstTxnStartTime := largestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05.000")

			sb.WriteString(fmt.Sprintf("\n**Transactions ending around when the first transaction started (%s):**\n\n",
				firstTxnStartTime))
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | End-to-Start | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------------|--------|----------------|\n")

			// Show the same number of transactions as in top lists
			earlyLimit := a.topTransactionCount
			if len(largestCluster.EarlyBlockers) < earlyLimit {
				earlyLimit = len(largestCluster.EarlyBlockers)
			}

			for i := 0; i < earlyLimit; i++ {
				txn := largestCluster.EarlyBlockers[i]

				// Calculate time between this transaction ending and first transaction starting
				timeDiff := largestCluster.FirstTransaction.StartTime.Sub(txn.EndTime)
				timeDiffStr := timeDiff.String()
				if timeDiff < 0 {
					// If negative (transaction ended after first transaction started), add a "+" prefix
					timeDiffStr = "+" + (-timeDiff).String()
				}

				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)

				sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %s | %d | %s |\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					timeDiffStr,
					len(txn.TablesAffected),
					strings.Join(tables, ", ")))
			}
		} else if largestCluster.FirstTransaction != nil {
			sb.WriteString(fmt.Sprintf("\n**No transactions found ending around when the first transaction started (%s).**\n\n",
				largestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05.000")))
		}

		// Count transactions by table
		tableCounts := make(map[string]int)
		for _, txn := range largestCluster.Transactions {
			for table := range txn.TablesAffected {
				tableCounts[table]++
			}
		}

		// Convert to slice for sorting
		type tableCount struct {
			Table string
			Count int
		}
		var tableCntSlice []tableCount
		for table, count := range tableCounts {
			tableCntSlice = append(tableCntSlice, tableCount{table, count})
		}

		// Sort by count (descending)
		sort.Slice(tableCntSlice, func(i, j int) bool {
			return tableCntSlice[i].Count > tableCntSlice[j].Count
		})

		// Print affected tables
		sb.WriteString("**Affected tables:**\n\n")
		sb.WriteString("| Table | Tx Count |\n")
		sb.WriteString("|-------|----------|\n")

		for _, tc := range tableCntSlice {
			sb.WriteString(fmt.Sprintf("| %s | %d |\n", tc.Table, tc.Count))
		}

		// Print the start and end times of a few transactions in the cluster as examples
		limit = a.topTransactionCount
		if len(largestCluster.Transactions) < limit {
			limit = len(largestCluster.Transactions)
		}

		sb.WriteString("\n**Sample transactions from this cluster:**\n\n")
		sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | Tables | Affected Tables |\n")
		sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------|----------------|\n")

		for i := 0; i < limit; i++ {
			txn := largestCluster.Transactions[i]

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

		// Display potential blocker transactions
		if len(largestCluster.PotentialBlockers) > 0 {
			// Get estimated blocking start time
			blockingStartTime := "unknown"
			if !largestCluster.ClusterStartTime.IsZero() {
				blockingStartTime = largestCluster.ClusterStartTime.Format("2006-01-02 15:04:05.000")
			}

			sb.WriteString(fmt.Sprintf("\n**Potential blocker transactions (ending close to when blocking started at %s):**\n\n",
				blockingStartTime))
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | End-to-Block | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------------|--------|----------------|\n")

			// Show same number of blockers as top transaction lists
			blockerLimit := a.topTransactionCount
			if len(largestCluster.PotentialBlockers) < blockerLimit {
				blockerLimit = len(largestCluster.PotentialBlockers)
			}

			for i := 0; i < blockerLimit; i++ {
				txn := largestCluster.PotentialBlockers[i]

				// Calculate time between this transaction ending and blocking starting
				var timeDiff time.Duration
				if !largestCluster.ClusterStartTime.IsZero() {
					timeDiff = largestCluster.ClusterStartTime.Sub(txn.EndTime)
				}

				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)

				sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %s | %d | %s |\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					timeDiff.String(),
					len(txn.TablesAffected),
					strings.Join(tables, ", ")))
			}
		} else {
			sb.WriteString("\n**No potential blocker transactions identified.**\n")
		}

		// Only show the longest cluster if it's different from the largest cluster
		if longestCluster.EndTime != largestCluster.EndTime {
			// Calculate avg duration for the longest cluster
			var totalDuration time.Duration
			for _, txn := range longestCluster.Transactions {
				totalDuration += txn.Duration
			}
			avgDuration := totalDuration / time.Duration(len(longestCluster.Transactions))

			blockingTimeInfo := ""
			if !longestCluster.ClusterStartTime.IsZero() {
				blockingTimeInfo = fmt.Sprintf(" (blocking time approximately %s)",
					longestCluster.ApproximateBlockingTime)
			}

			sb.WriteString(fmt.Sprintf("\n\n#### Details for longest cluster by average duration (%d transactions, avg: %s, ending around %s)%s\n\n",
				len(longestCluster.Transactions),
				avgDuration.String(),
				longestCluster.EndTime.Format("2006-01-02 15:04:05.000"),
				blockingTimeInfo))

			// Print information about the first transaction in this cluster
			if longestCluster.FirstTransaction != nil {
				sb.WriteString("**First transaction in cluster:**\n\n")
				sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | Tables | Affected Tables |\n")
				sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------|----------------|\n")

				// Get affected tables
				var tables []string
				for table := range longestCluster.FirstTransaction.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)

				sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %d | %s |\n",
					longestCluster.FirstTransaction.XID,
					longestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05"),
					longestCluster.FirstTransaction.EndTime.Format("2006-01-02 15:04:05"),
					longestCluster.FirstTransaction.Duration.String(),
					longestCluster.FirstTransaction.RowsChanged,
					formatByteSize(longestCluster.FirstTransaction.ByteSize),
					len(longestCluster.FirstTransaction.TablesAffected),
					strings.Join(tables, ", ")))
			}

			// Display transactions that ended around when the first transaction started
			if longestCluster.FirstTransaction != nil && len(longestCluster.EarlyBlockers) > 0 {
				firstTxnStartTime := longestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05.000")

				sb.WriteString(fmt.Sprintf("\n**Transactions ending around when the first transaction started (%s):**\n\n",
					firstTxnStartTime))
				sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | End-to-Start | Tables | Affected Tables |\n")
				sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------------|--------|----------------|\n")

				// Show same number of transactions as top lists
				earlyLimit := a.topTransactionCount
				if len(longestCluster.EarlyBlockers) < earlyLimit {
					earlyLimit = len(longestCluster.EarlyBlockers)
				}

				for i := 0; i < earlyLimit; i++ {
					txn := longestCluster.EarlyBlockers[i]

					// Calculate time between this transaction ending and first transaction starting
					timeDiff := longestCluster.FirstTransaction.StartTime.Sub(txn.EndTime)
					timeDiffStr := timeDiff.String()
					if timeDiff < 0 {
						// If negative (transaction ended after first transaction started), add a "+" prefix
						timeDiffStr = "+" + (-timeDiff).String()
					}

					// Get affected tables
					var tables []string
					for table := range txn.TablesAffected {
						tables = append(tables, table)
					}
					sort.Strings(tables)

					sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %s | %d | %s |\n",
						txn.XID,
						txn.StartTime.Format("2006-01-02 15:04:05"),
						txn.EndTime.Format("2006-01-02 15:04:05"),
						txn.Duration.String(),
						txn.RowsChanged,
						formatByteSize(txn.ByteSize),
						timeDiffStr,
						len(txn.TablesAffected),
						strings.Join(tables, ", ")))
				}
			}

			// Print sample transactions from the longest cluster
			limit = a.topTransactionCount
			if len(longestCluster.Transactions) < limit {
				limit = len(longestCluster.Transactions)
			}

			sb.WriteString("\n**Sample transactions from this cluster:**\n\n")
			sb.WriteString("| Transaction ID | Start Time | End Time | Duration | Rows | Bytes | Tables | Affected Tables |\n")
			sb.WriteString("|----------------|-----------|----------|----------|------|-------|--------|----------------|\n")

			for i := 0; i < limit; i++ {
				txn := longestCluster.Transactions[i]

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
		}
	}

	sb.WriteString("\n")
}

// printTransactionClusters prints information about detected transaction clusters
func (a *Analyzer) printTransactionClusters() {
	clusters := a.findTransactionClusters()
	if len(clusters) == 0 {
		return
	}

	fmt.Println("\n=== Transaction End Time Cluster Analysis ===")
	fmt.Printf("Detected %d clusters of transactions with similar end times (window: %s)\n",
		len(clusters), a.clusterTimeWindow)

	// Print the largest clusters (top 5 or less)
	limit := 5
	if len(clusters) < limit {
		limit = len(clusters)
	}

	fmt.Println("\nTop clusters by transaction count (potential blocking events):")
	fmt.Printf("%-25s | %-10s | %-15s | %-20s | %-15s | %-15s\n",
		"Cluster End Time", "Tx Count", "Blocking Time", "Avg Duration", "Min Duration", "Max Duration")
	fmt.Println(strings.Repeat("-", 115))

	for i := 0; i < limit; i++ {
		cluster := clusters[i]

		// Calculate statistics about the cluster
		var totalDuration time.Duration
		minDuration := cluster.Transactions[0].Duration
		maxDuration := cluster.Transactions[0].Duration

		for _, txn := range cluster.Transactions {
			totalDuration += txn.Duration

			if txn.Duration < minDuration {
				minDuration = txn.Duration
			}
			if txn.Duration > maxDuration {
				maxDuration = txn.Duration
			}
		}

		avgDuration := totalDuration / time.Duration(len(cluster.Transactions))

		blockingTime := "N/A"
		if !cluster.ClusterStartTime.IsZero() {
			blockingTime = cluster.ApproximateBlockingTime.String()
		}

		fmt.Printf("%-25s | %-10d | %-15s | %-20s | %-15s | %-15s\n",
			cluster.EndTime.Format("2006-01-02 15:04:05.000"),
			len(cluster.Transactions),
			blockingTime,
			avgDuration.String(),
			minDuration.String(),
			maxDuration.String())
	}

	// For clusters, show details about the largest cluster and the longest cluster
	if len(clusters) > 0 {
		// Find largest cluster (most transactions)
		largestCluster := clusters[0]

		// Find longest cluster by average duration
		longestCluster := clusters[0]
		var maxAvgDuration time.Duration

		for _, cluster := range clusters {
			var totalDuration time.Duration
			for _, txn := range cluster.Transactions {
				totalDuration += txn.Duration
			}
			avgDuration := totalDuration / time.Duration(len(cluster.Transactions))

			if avgDuration > maxAvgDuration {
				maxAvgDuration = avgDuration
				longestCluster = cluster
			}
		}

		// Print details for the largest cluster (by transaction count)
		blockingTimeInfo := ""
		if !largestCluster.ClusterStartTime.IsZero() {
			blockingTimeInfo = fmt.Sprintf(" - blocking time approximately %s",
				largestCluster.ApproximateBlockingTime)
		}

		fmt.Printf("\nDetails for largest cluster (%d transactions ending around %s%s):\n",
			len(largestCluster.Transactions),
			largestCluster.EndTime.Format("2006-01-02 15:04:05.000"),
			blockingTimeInfo)

		// Print information about the first transaction in the cluster
		if largestCluster.FirstTransaction != nil {
			fmt.Println("\nFirst transaction in cluster:")
			fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n",
				"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
			fmt.Println(strings.Repeat("-", 160))

			// Get affected tables
			var tables []string
			for table := range largestCluster.FirstTransaction.TablesAffected {
				tables = append(tables, table)
			}
			sort.Strings(tables)

			fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-10d | %s\n",
				largestCluster.FirstTransaction.XID,
				largestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05"),
				largestCluster.FirstTransaction.EndTime.Format("2006-01-02 15:04:05"),
				largestCluster.FirstTransaction.Duration.String(),
				largestCluster.FirstTransaction.RowsChanged,
				formatByteSize(largestCluster.FirstTransaction.ByteSize),
				len(largestCluster.FirstTransaction.TablesAffected),
				strings.Join(tables, ", "))
		}

		// Display transactions that ended around when the first transaction started
		if largestCluster.FirstTransaction != nil && len(largestCluster.EarlyBlockers) > 0 {
			firstTxnStartTime := largestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05.000")

			fmt.Printf("\nTransactions ending around when the first transaction started (%s):\n",
				firstTxnStartTime)
			fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-15s | %-10s | %s\n",
				"Transaction ID", "Start Time", "End Time", "Duration", "Rows", "Bytes", "End-to-Start", "Tables", "Affected Tables")
			fmt.Println(strings.Repeat("-", 180))

			// Show the same number of blockers as top transaction lists
			earlyLimit := a.topTransactionCount
			if len(largestCluster.EarlyBlockers) < earlyLimit {
				earlyLimit = len(largestCluster.EarlyBlockers)
			}

			for i := 0; i < earlyLimit; i++ {
				txn := largestCluster.EarlyBlockers[i]

				// Calculate time between this transaction ending and first transaction starting
				timeDiff := largestCluster.FirstTransaction.StartTime.Sub(txn.EndTime)
				timeDiffStr := timeDiff.String()
				if timeDiff < 0 {
					// If negative (transaction ended after first transaction started), add a "+" prefix
					timeDiffStr = "+" + (-timeDiff).String()
				}

				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)

				fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-15s | %-10d | %s\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					timeDiffStr,
					len(txn.TablesAffected),
					strings.Join(tables, ", "))
			}
		} else if largestCluster.FirstTransaction != nil {
			fmt.Printf("\nNo transactions found ending around when the first transaction started (%s).\n",
				largestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05.000"))
		}

		// Count transactions by table
		tableCounts := make(map[string]int)
		for _, txn := range largestCluster.Transactions {
			for table := range txn.TablesAffected {
				tableCounts[table]++
			}
		}

		// Convert to slice for sorting
		type tableCount struct {
			Table string
			Count int
		}
		var tableCntSlice []tableCount
		for table, count := range tableCounts {
			tableCntSlice = append(tableCntSlice, tableCount{table, count})
		}

		// Sort by count (descending)
		sort.Slice(tableCntSlice, func(i, j int) bool {
			return tableCntSlice[i].Count > tableCntSlice[j].Count
		})

		// Print affected tables
		fmt.Println("\nAffected tables:")
		fmt.Printf("%-30s | %-10s\n", "Table", "Tx Count")
		fmt.Println(strings.Repeat("-", 45))

		for _, tc := range tableCntSlice {
			fmt.Printf("%-30s | %-10d\n", tc.Table, tc.Count)
		}

		// Print the start and end times of transactions in the cluster as examples
		limit := a.topTransactionCount
		if len(largestCluster.Transactions) < limit {
			limit = len(largestCluster.Transactions)
		}

		fmt.Println("\nSample transactions from this cluster:")
		fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n",
			"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
		fmt.Println(strings.Repeat("-", 160))

		for i := 0; i < limit; i++ {
			txn := largestCluster.Transactions[i]

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

		// Display potential blocker transactions
		if len(largestCluster.PotentialBlockers) > 0 {
			// Get estimated blocking start time
			blockingStartTime := "unknown"
			if !largestCluster.ClusterStartTime.IsZero() {
				blockingStartTime = largestCluster.ClusterStartTime.Format("2006-01-02 15:04:05.000")
			}

			fmt.Printf("\nPotential blocker transactions (ending close to when blocking started at %s):\n",
				blockingStartTime)
			fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-15s | %-10s | %s\n",
				"Transaction ID", "Start Time", "End Time", "Duration", "Rows", "Bytes", "End-to-Block", "Tables", "Affected Tables")
			fmt.Println(strings.Repeat("-", 180))

			// Show same number of blockers as top transaction lists
			blockerLimit := a.topTransactionCount
			if len(largestCluster.PotentialBlockers) < blockerLimit {
				blockerLimit = len(largestCluster.PotentialBlockers)
			}

			for i := 0; i < blockerLimit; i++ {
				txn := largestCluster.PotentialBlockers[i]

				// Calculate time between this transaction ending and blocking starting
				var timeDiff time.Duration
				if !largestCluster.ClusterStartTime.IsZero() {
					timeDiff = largestCluster.ClusterStartTime.Sub(txn.EndTime)
				}

				// Get affected tables
				var tables []string
				for table := range txn.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)

				fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-15s | %-10d | %s\n",
					txn.XID,
					txn.StartTime.Format("2006-01-02 15:04:05"),
					txn.EndTime.Format("2006-01-02 15:04:05"),
					txn.Duration.String(),
					txn.RowsChanged,
					formatByteSize(txn.ByteSize),
					timeDiff.String(),
					len(txn.TablesAffected),
					strings.Join(tables, ", "))
			}
		} else {
			fmt.Println("\nNo potential blocker transactions identified.")
		}

		// Only show the longest cluster if it's different from the largest cluster
		if longestCluster.EndTime != largestCluster.EndTime {
			blockingTimeInfo = ""
			if !longestCluster.ClusterStartTime.IsZero() {
				blockingTimeInfo = fmt.Sprintf(" - blocking time approximately %s",
					longestCluster.ApproximateBlockingTime)
			}

			// Calculate avg duration for the longest cluster
			var totalDuration time.Duration
			for _, txn := range longestCluster.Transactions {
				totalDuration += txn.Duration
			}
			avgDuration := totalDuration / time.Duration(len(longestCluster.Transactions))

			fmt.Printf("\n\nDetails for longest cluster by average duration (%d transactions, avg: %s, ending around %s%s):\n",
				len(longestCluster.Transactions),
				avgDuration.String(),
				longestCluster.EndTime.Format("2006-01-02 15:04:05.000"),
				blockingTimeInfo)

			// Print information about the first transaction in the longest cluster
			if longestCluster.FirstTransaction != nil {
				fmt.Println("\nFirst transaction in cluster:")
				fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n",
					"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
				fmt.Println(strings.Repeat("-", 160))

				// Get affected tables
				var tables []string
				for table := range longestCluster.FirstTransaction.TablesAffected {
					tables = append(tables, table)
				}
				sort.Strings(tables)

				fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-10d | %s\n",
					longestCluster.FirstTransaction.XID,
					longestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05"),
					longestCluster.FirstTransaction.EndTime.Format("2006-01-02 15:04:05"),
					longestCluster.FirstTransaction.Duration.String(),
					longestCluster.FirstTransaction.RowsChanged,
					formatByteSize(longestCluster.FirstTransaction.ByteSize),
					len(longestCluster.FirstTransaction.TablesAffected),
					strings.Join(tables, ", "))
			}

			// Display the same type of information for the longest cluster
			if len(longestCluster.EarlyBlockers) > 0 && longestCluster.FirstTransaction != nil {
				firstTxnStartTime := longestCluster.FirstTransaction.StartTime.Format("2006-01-02 15:04:05.000")

				fmt.Printf("\nTransactions ending around when the first transaction started (%s):\n",
					firstTxnStartTime)
				fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-15s | %-10s | %s\n",
					"Transaction ID", "Start Time", "End Time", "Duration", "Rows", "Bytes", "End-to-Start", "Tables", "Affected Tables")
				fmt.Println(strings.Repeat("-", 180))

				// Show the same number of blockers as top transaction lists
				earlyLimit := a.topTransactionCount
				if len(longestCluster.EarlyBlockers) < earlyLimit {
					earlyLimit = len(longestCluster.EarlyBlockers)
				}

				for i := 0; i < earlyLimit; i++ {
					txn := longestCluster.EarlyBlockers[i]

					// Calculate time between this transaction ending and first transaction starting
					timeDiff := longestCluster.FirstTransaction.StartTime.Sub(txn.EndTime)
					timeDiffStr := timeDiff.String()
					if timeDiff < 0 {
						// If negative (transaction ended after first transaction started), add a "+" prefix
						timeDiffStr = "+" + (-timeDiff).String()
					}

					// Get affected tables
					var tables []string
					for table := range txn.TablesAffected {
						tables = append(tables, table)
					}
					sort.Strings(tables)

					fmt.Printf("%-20d | %-20s | %-20s | %-15s | %-10d | %-15s | %-15s | %-10d | %s\n",
						txn.XID,
						txn.StartTime.Format("2006-01-02 15:04:05"),
						txn.EndTime.Format("2006-01-02 15:04:05"),
						txn.Duration.String(),
						txn.RowsChanged,
						formatByteSize(txn.ByteSize),
						timeDiffStr,
						len(txn.TablesAffected),
						strings.Join(tables, ", "))
				}
			}

			// Print sample transactions from the longest cluster
			limit = a.topTransactionCount
			if len(longestCluster.Transactions) < limit {
				limit = len(longestCluster.Transactions)
			}

			fmt.Println("\nSample transactions from this cluster:")
			fmt.Printf("%-20s | %-20s | %-20s | %-15s | %-10s | %-15s | %-10s | %s\n",
				"Transaction ID", "Start Time", "End Time", "Duration", "Rows Changed", "Bytes", "Tables", "Affected Tables")
			fmt.Println(strings.Repeat("-", 160))

			for i := 0; i < limit; i++ {
				txn := longestCluster.Transactions[i]

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
	}

	fmt.Println()
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
