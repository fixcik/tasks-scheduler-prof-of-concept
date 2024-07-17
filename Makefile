PUBLISHER=bin/publisher
SCHEDULER=bin/scheduler

# Rules
all: build

build:
	go build -o $(PUBLISHER) cmd/publisher/main.go
	go build -o $(SCHEDULER) cmd/scheduler/main.go

clean:
	rm -f $(PUBLISHER) $(SCHEDULER)