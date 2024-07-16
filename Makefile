PUBLISHER=publisher
SCHEDULER=scheduler

# Rules
all: build

build:
	go build -o bin/$(PUBLISHER) cmd/publisher/main.go
	go build -o bin/$(SCHEDULER) cmd/scheduler/main.go

clean:
	rm -f $(PUBLISHER) $(SCHEDULER)