build:
	go build -ldflags "-w -s -X github.com/worldbug/kafeman/internal/command.commit=debug -X github.com/worldbug/kafeman/internal/command.version=debug" -o builds/kafeman cmd/kafeman/main.go