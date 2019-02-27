VERSION?=git-$(shell git rev-parse --abbrev-ref HEAD)-$(shell git rev-parse --short HEAD).$(shell date +%Y-%m-%d.%H-%M-%S)

.PHONY: all
all: build

vendor: Gopkg.lock Gopkg.toml
	dep ensure -vendor-only

.PHONY: build
build: vendor
	go build -ldflags "-X main.version=$(VERSION)" -o bin/go-mysql-sphinx ./cmd/go-mysql-sphinx/

.PHONY: test
test: vendor
	go test $(TEST_FLAGS) ./river/
