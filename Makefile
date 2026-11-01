BINARY=litepay
CONFIG=config.yaml

.PHONY: build run test tidy clean linux

build:
	go build -o $(BINARY) .

run: build
	./$(BINARY) -config $(CONFIG)

test:
	go test ./...

tidy:
	go mod tidy

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -o dist/$(BINARY)-linux-amd64 .

clean:
	rm -rf dist $(BINARY)
