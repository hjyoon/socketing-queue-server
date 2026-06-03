.PHONY: test coverage

test:
	GOCACHE=/tmp/socketing-queue-go-cache go test -mod=mod ./...

coverage:
	GOCACHE=/tmp/socketing-queue-go-cache go test -mod=mod \
		./internal/app ./internal/auth \
		-coverpkg=./internal/app,./internal/auth \
		-coverprofile=coverage.out
	GOCACHE=/tmp/socketing-queue-go-cache go tool cover -func=coverage.out | tee /tmp/socketing-queue-coverage.txt
	awk '/total:/ {gsub(/%/,"",$$3); if ($$3 < 97) exit 1}' /tmp/socketing-queue-coverage.txt
