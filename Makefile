PROJECT             := github.com/amazingchow/snippets-for-gopher
SRC                 := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
ELECTLEADER_TARGETS := node_01 node_02 node_03
ALL_TARGETS         := $(ELECTLEADER_TARGETS)

all: build

build: $(ALL_TARGETS)

$(ELECTLEADER_TARGETS): $(SRC)
	go build $(GOMODULEPATH)/$(PROJECT)/cmd/electleader/$@

test:
	go test -count=1 -v -p 1 $(shell go list ./...)

clean:
	rm -f $(ALL_TARGETS)

.PHONY: all build clean
