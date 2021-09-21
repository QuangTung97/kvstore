.PHONY: lint test install-tools generate

lint:
	go fmt ./...
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -v -tags integration ./...


install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive
	go install github.com/gogo/protobuf/protoc-gen-gofast

WORKDIR := ${PWD}
PROTO_DIR := ${WORKDIR}/proto
GENERATED_DIR := ${WORKDIR}/kvstorepb

generate:
	cd ${PROTO_DIR} && \
		protoc --gofast_out=${GENERATED_DIR} --gofast_opt=paths=source_relative command.proto