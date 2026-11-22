DB_MIGRATE_URL = postgres://login:pass@localhost:5432/postgres?sslmode=disable
MIGRATE_PATH = ./migration/postgres

LOCAL_BIN := $(CURDIR)/bin

PROTOC_VERSION = 36.2
GOLANGCI_LINT_VERSION = v2.15.0
MIGRATE_VERSION = v4.20.1

PROTOC := $(LOCAL_BIN)/protoc/bin/protoc
PROTOC_OS := $(shell uname -s | sed 's/Darwin/osx/;s/Linux/linux/')
PROTOC_ARCH := $(shell uname -m | sed 's/arm64/aarch_64/;s/aarch64/aarch_64/')

lint:
	$(LOCAL_BIN)/golangci-lint run

.PHONY: test
test:
	go test -race -shuffle=on ./...

integration-test:
	go test -count=1 -tags=integration -timeout=20m ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

bin-deps: protoc-install lint-install migrate-install

protoc-install:
	mkdir -p $(LOCAL_BIN)
	curl -sSfL -o $(LOCAL_BIN)/protoc.zip \
	  https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)-$(PROTOC_ARCH).zip
	rm -rf $(LOCAL_BIN)/protoc
	unzip -q $(LOCAL_BIN)/protoc.zip -d $(LOCAL_BIN)/protoc
	rm $(LOCAL_BIN)/protoc.zip
	GOBIN=$(LOCAL_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc

lint-install:
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(LOCAL_BIN) $(GOLANGCI_LINT_VERSION)

migrate-install:
	GOBIN=$(LOCAL_BIN) go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)
