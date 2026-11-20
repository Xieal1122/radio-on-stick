.PHONY: test build station relay directory relay-agent dist

VERSION ?= 0.1.0
BIN := bin

test:
	go test ./...

build: station relay directory relay-agent

station:
	mkdir -p $(BIN)
	go build -trimpath -ldflags="-s -w" -o $(BIN)/radio-station-go ./cmd/station

relay:
	mkdir -p $(BIN)
	go build -trimpath -ldflags="-s -w" -o $(BIN)/radio-relay ./cmd/relay

relay-agent:
	mkdir -p $(BIN)
	go build -trimpath -ldflags="-s -w" -o $(BIN)/radio-relay-agent ./cmd/relay-agent

directory:
	mkdir -p $(BIN)
	go build -trimpath -ldflags="-s -w" -o $(BIN)/radio-directory ./cmd/directory

dist:
	RADIO_VERSION=$(VERSION) bash deploy/package.sh
