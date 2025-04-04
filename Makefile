.PHONY: build test clean

build:
	go build -o bin/binlog-analyze github.com/miles/binlog-analyze/cmd/binlog-analyze

test:
	./run-tests.sh

clean:
	rm -rf bin

install: build
	mv bin/binlog-analyze /usr/local/bin/

all: clean test build