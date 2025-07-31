.PHONY: tidy test coverage

tidy:
	@go mod tidy

test: tidy
	go test ./...

coverage: tidy
	@go test -cover -coverpkg=./... ./... 2>/dev/null | grep "coverage:" | head -1 | awk '{print "Total Coverage: " $$5}'

