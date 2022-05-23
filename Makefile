GOFILES=**/*.go *.go

coverage: $(GOFILES)
	go test ./... -coverprofile=coverage

show-cov: coverage
	go tool cover -html coverage

test: $(GOFILES)
	go test -v ./... -race

clean:
	rm coverage

check: $(GOFILES)
	staticcheck ./...
	golint ./...

# give 'er the ol' one two 'innit
one-two: test check show-cov

.PHONY=clean test show-cov check
