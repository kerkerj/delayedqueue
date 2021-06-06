test:
	go test -v ./...

test-cov:
	go test -v -cover -covermode=count -coverprofile=cover.out ./...

lint:
	golangci-lint run -v ./... --config=.golangci.yml --out-format=line-number --issues-exit-code=0