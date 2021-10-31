.PHONY: all

ldflags := -X 'github.com/livepeer/cdn-log-puller/model.Version=$(shell git describe --dirty)'

all:
	go build -o cdn-pull -ldflags="$(ldflags)" cmd/cdn-pull/cdn-pull.go 

