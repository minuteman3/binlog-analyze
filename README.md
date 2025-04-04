# MySQL Binlog Analyzer

[![Tests](https://github.com/miles/binlog-analyze/actions/workflows/tests.yml/badge.svg)](https://github.com/miles/binlog-analyze/actions/workflows/tests.yml)
[![Code Coverage](https://github.com/miles/binlog-analyze/actions/workflows/coverage.yml/badge.svg)](https://github.com/miles/binlog-analyze/actions/workflows/coverage.yml)
[![Lint](https://github.com/miles/binlog-analyze/actions/workflows/lint.yml/badge.svg)](https://github.com/miles/binlog-analyze/actions/workflows/lint.yml)

A Go-based tool for analyzing MySQL binary logs and providing summary statistics.

## Features

- Analyzes MySQL binlog files (.bin files)
- Provides summary statistics of database operations
- Counts inserts, updates, and deletes per table
- Shows total number of rows changed
- Tracks transaction sizes in bytes
- Shows transaction start and end times
- Detects clusters of transactions that end at similar times (potential blocking events)
- Analyzes and merges multiple binlog files
- Outputs results in plain text or markdown format

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/binlog-analyze.git
cd binlog-analyze

# Build the application
go build -o binlog-analyze ./cmd/binlog-analyze
```

Or install directly with Go:

```bash
go install github.com/miles/binlog-analyze/cmd/binlog-analyze@latest
```

## Usage

```bash
# Basic usage with a single file
binlog-analyze --file=/path/to/mysql-bin.000001

# Analyze multiple files and merge results
binlog-analyze --files=/path/to/mysql-bin.000001,/path/to/mysql-bin.000002

# Output in markdown format
binlog-analyze --file=/path/to/mysql-bin.000001 --format=markdown

# Save output to a file
binlog-analyze --file=/path/to/mysql-bin.000001 --output=report.txt

# Save markdown output to a file
binlog-analyze --file=/path/to/mysql-bin.000001 --format=markdown --output=report.md

# Only show transactions that took at least 500ms
binlog-analyze --file=/path/to/mysql-bin.000001 --min-duration=500ms

# Analyze multiple files and filter by duration
binlog-analyze --files=/path/to/mysql-bin.000001,/path/to/mysql-bin.000002 --min-duration=1s

# Show top 20 transactions in each category (default is 10)
binlog-analyze --file=/path/to/mysql-bin.000001 --top=20

# Detect clusters of transactions that end at similar times (potential blocking events)
binlog-analyze --file=/path/to/mysql-bin.000001 --detect-clusters

# Customize the time window for cluster detection (default is 1s)
binlog-analyze --file=/path/to/mysql-bin.000001 --detect-clusters --cluster-window=500ms
```

## Example Output

### Text Format

```
=== MySQL Binlog Analysis Summary ===
Binlog File: /var/lib/mysql/mysql-bin.000001
Time Range: 2025-04-04 10:00:00 to 2025-04-04 11:00:00 (Duration: 1h0m0s)
Total Events: 15243
Total Rows Changed: 8721
Total Bytes: 25678912 (24.49 MB)
Total Transactions: 423

=== Table Statistics ===
Table                           | Inserts     | Updates     | Deletes     | Total      
--------------------------------------------------------------------------------
users                           | 120         | 350         | 45          | 515        
products                        | 250         | 125         | 10          | 385        
orders                          | 600         | 200         | 5           | 805

=== Transaction Statistics ===
Showing top 10 longest transactions:
Transaction ID         | Start Time           | End Time             | Duration        | Rows Changed | Bytes           | Tables     | Affected Tables
---------------------------------------------------------------------------------------------------------------------------------------------------------------
54321                  | 2025-04-04 10:15:30 | 2025-04-04 10:15:33 | 2.458s          | 342          | 1.25 MB         | 2          | users, orders
12345                  | 2025-04-04 10:20:15 | 2025-04-04 10:20:17 | 1.876s          | 215          | 850.32 KB       | 3          | users, products, orders
```

### Markdown Format

A sample of the markdown output:

```markdown
# MySQL Binlog Analysis Report

**Binlog File:** /var/lib/mysql/mysql-bin.000001

**Time Range:** 2025-04-04 10:00:00 to 2025-04-04 11:00:00 (Duration: 1h0m0s)

**Total Events:** 15243

**Total Rows Changed:** 8721

**Total Bytes:** 25678912 (24.49 MB)

**Total Transactions:** 423

## Table Statistics

| Table | Inserts | Updates | Deletes | Total |
|-------|---------|---------|---------|-------|
| users | 120 | 350 | 45 | 515 |
| products | 250 | 125 | 10 | 385 |
| orders | 600 | 200 | 5 | 805 |

## Transaction Statistics

Top 10 longest transactions:

| Transaction ID | Start Time | End Time | Duration | Rows Changed | Bytes | Tables | Affected Tables |
|----------------|-----------|---------|----------|--------------|-------|--------|----------------|
| 54321 | 2025-04-04 10:15:30 | 2025-04-04 10:15:33 | 2.458s | 342 | 1.25 MB | 2 | users, orders |
| 12345 | 2025-04-04 10:20:15 | 2025-04-04 10:20:17 | 1.876s | 215 | 850.32 KB | 3 | users, products, orders |
```

## Requirements

- Go 1.19 or higher

## License

MIT