.PHONY: build test vendor tidy

build:
	go build -mod=vendor -v ./...

test:
	go test -mod=vendor ./...

tidy:
	go mod tidy

vendor:
	go mod vendor
