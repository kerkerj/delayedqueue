test:
	go test -v ./...

test-cov:
	go test `go list ./... | grep -v example` -coverprofile=cover.out -covermode=atomic

lint:
	golangci-lint run -v ./... --config=.golangci.yml --out-format=line-number --issues-exit-code=0