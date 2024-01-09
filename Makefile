test:
	go test -v -race ./...

run:
	go run cmd/server/demo.go

client:
	go run cmd/client/client.go

lint:
	golangci-lint --config=.golangci.yaml run --fix

gen:
	@$(MAKE)  -C proto gen
