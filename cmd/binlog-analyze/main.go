package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/miles/binlog-analyze/pkg/analyzer"
	"github.com/miles/binlog-analyze/pkg/models"
	"github.com/miles/binlog-analyze/pkg/parser"
)

func main() {
	// Define command-line flags
	binlogFile := flag.String("file", "", "Path to MySQL binlog file")
	binlogFiles := flag.String("files", "", "Comma-separated list of MySQL binlog files to merge")
	outputFormat := flag.String("format", "text", "Output format: text or markdown")
	outputFile := flag.String("output", "", "Optional file to write output to")
	minDuration := flag.Duration("min-duration", 0, "Only show transactions with at least this duration (e.g. 100ms, 1s, 500ms)")
	topCount := flag.Int("top", 10, "Number of transactions to show in top lists (default: 10)")
	detectClusters := flag.Bool("detect-clusters", false, "Detect clusters of transactions with similar end times (potential blocking events)")
	clusterWindow := flag.Duration("cluster-window", time.Second, "Time window to consider transactions as part of the same cluster (default: 1s)")
	flag.Parse()

	// Check if at least one binlog file option is provided
	if *binlogFile == "" && *binlogFiles == "" {
		fmt.Println("Error: Missing required parameter --file or --files")
		printUsage()
		os.Exit(1)
	}

	var filesToProcess []string

	// Process single file
	if *binlogFile != "" {
		// Check if file exists
		if _, err := os.Stat(*binlogFile); os.IsNotExist(err) {
			fmt.Printf("Error: Binlog file '%s' does not exist\n", *binlogFile)
			os.Exit(1)
		}
		filesToProcess = append(filesToProcess, *binlogFile)
	}

	// Process multiple files
	if *binlogFiles != "" {
		fileList := strings.Split(*binlogFiles, ",")
		for _, file := range fileList {
			file = strings.TrimSpace(file)
			if file == "" {
				continue
			}

			// Check if file exists
			if _, err := os.Stat(file); os.IsNotExist(err) {
				fmt.Printf("Error: Binlog file '%s' does not exist\n", file)
				os.Exit(1)
			}
			filesToProcess = append(filesToProcess, file)
		}
	}

	// Initialize an empty stats object
	var combinedStats *models.BinlogStats

	// Process each file and merge stats
	for i, file := range filesToProcess {
		// Initialize parser
		p := parser.NewParser(file)

		// Parse binlog file
		fmt.Printf("Analyzing binlog file (%d/%d): %s\n", i+1, len(filesToProcess), file)
		stats, err := p.ParseBinlog()
		if err != nil {
			fmt.Printf("Error parsing binlog: %v\n", err)
			os.Exit(1)
		}

		if combinedStats == nil {
			// For the first file, just use its stats
			combinedStats = stats
		} else {
			// For subsequent files, merge the stats
			combinedStats.MergeWith(stats)
		}
	}

	// Use the combined stats for analysis
	stats := combinedStats

	// Initialize analyzer with filtering options
	a := analyzer.NewAnalyzer(stats)

	// Set minimum transaction duration filter if provided
	if *minDuration > 0 {
		a.SetMinTransactionDuration(*minDuration)
	}

	// Set top transaction count
	a.SetTopTransactionCount(*topCount)

	// Enable transaction cluster detection if requested
	if *detectClusters {
		a.EnableTransactionClusterDetection(true)
		a.SetClusterTimeWindow(*clusterWindow)
	}

	// Generate output
	var output string
	if *outputFormat == "markdown" {
		output = a.GetMarkdownReport()
	} else {
		// Capture text output
		a.PrintSummary()
		if *outputFile == "" {
			// If no output file is specified, we already printed to stdout
			return
		}
	}

	// Write to output file if specified
	if *outputFile != "" {
		err := writeToFile(*outputFile, output)
		if err != nil {
			fmt.Printf("Error writing to output file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Output written to: %s\n", *outputFile)
	} else if *outputFormat == "markdown" {
		// Print markdown to stdout if no output file
		fmt.Println(output)
	}
}

// writeToFile writes content to a file
func writeToFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

// printUsage prints usage information
func printUsage() {
	fmt.Println("\nUsage: binlog-analyze [--file=<binlog-file> | --files=<comma-separated-binlog-files>] [--format=text|markdown] [--output=<output-file>] [--min-duration=<duration>] [--top=<count>] [--detect-clusters] [--cluster-window=<duration>]")
	fmt.Println("\nParameters:")
	fmt.Println("  --file           Path to a single MySQL binlog file")
	fmt.Println("  --files          Comma-separated list of MySQL binlog files to analyze and merge")
	fmt.Println("  --format         Output format: text or markdown (default: text)")
	fmt.Println("  --output         Optional file to write output to")
	fmt.Println("  --min-duration   Only show transactions with at least this duration (e.g. 100ms, 1s, 500ms)")
	fmt.Println("  --top            Number of transactions to show in top lists (default: 10)")
	fmt.Println("  --detect-clusters Detect clusters of transactions that end at similar times (potential blocking events)")
	fmt.Println("  --cluster-window Time window to consider transactions as part of the same cluster (default: 1s)")
	fmt.Println("\nExample:")
	fmt.Println("  binlog-analyze --file=/var/lib/mysql/mysql-bin.000001")
	fmt.Println("  binlog-analyze --files=/var/lib/mysql/mysql-bin.000001,/var/lib/mysql/mysql-bin.000002")
	fmt.Println("  binlog-analyze --file=/var/lib/mysql/mysql-bin.000001 --format=markdown --output=report.md")
	fmt.Println("  binlog-analyze --files=/var/lib/mysql/mysql-bin.000001,/var/lib/mysql/mysql-bin.000002 --min-duration=500ms")
	fmt.Println("  binlog-analyze --file=/var/lib/mysql/mysql-bin.000001 --top=20 # Show top 20 transactions")
	fmt.Println("  binlog-analyze --file=/var/lib/mysql/mysql-bin.000001 --detect-clusters --cluster-window=500ms")
}
