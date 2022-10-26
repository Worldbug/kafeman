build:
	go build -ldflags "-w -s" ./cmd/gkat
install:
	go install -ldflags "-w -s" ./cmd/gkat
release:
	goreleaser --rm-dist