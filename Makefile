.PHONY: test coverage clean

# Run all tests
test:
	go test ./...

# Get test coverage percentage for the entire project
coverage:
	@echo "Getting test coverage for entire project..."
	@go test -cover -coverpkg=./... ./... 2>/dev/null | grep "coverage:" | head -1 | awk '{print "Total Coverage: " $$5}'
