PROTO_ROOT    := proto
PROTO_FILES   := $(shell find $(PROTO_ROOT) -name "*.proto")
MODULE        := github.com/accretional/grpc-server-config

# Upstream repos whose proto sources are resolved at generation time.
# These paths must be accessible locally (e.g. via go.mod replace directives).
GLUON_ROOT    := ../gluon
PROTO_SYM_ROOT := ../proto-symbol

.PHONY: generate build test clean install-plugins

# Install protoc plugins. Run once after cloning.
install-plugins:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate Go pb files from all protos under proto/.
# Output lands in proto/<pkg>/pb/ to match go_package options.
generate:
	protoc \
		-I $(PROTO_ROOT) \
		-I $(GLUON_ROOT) \
		-I $(PROTO_SYM_ROOT) \
		--go_out=. \
		--go_opt=module=$(MODULE) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(MODULE) \
		--go-grpc_opt=require_unimplemented_servers=false \
		$(PROTO_FILES)

build:
	go build ./...

test:
	go test ./...

# Remove all generated pb files. Re-run make generate to restore.
clean:
	find . -path '*/pb/*.pb.go' -delete
