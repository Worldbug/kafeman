build:
	go build -ldflags "-w -s" ./cmd/protokaf
install:
	go install -ldflags "-w -s" ./cmd/protokaf
release:
	goreleaser --rm-dist