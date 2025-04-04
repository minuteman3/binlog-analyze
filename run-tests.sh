#!/bin/bash

echo "Running all tests for binlog-analyze..."
echo

echo "Running models tests..."
go test -v github.com/miles/binlog-analyze/pkg/models
echo

echo "Running analyzer tests..."
go test -v github.com/miles/binlog-analyze/pkg/analyzer
echo

echo "Running parser tests..."
go test -v github.com/miles/binlog-analyze/pkg/parser
echo

echo "Running main package tests..."
go test -v github.com/miles/binlog-analyze/cmd/binlog-analyze
echo

echo "All tests completed."