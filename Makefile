commands = realize

.PHONY: all
all: $(commands) test

.PHONY: test
test:
	go test $$(go list ./... | grep -v cmd)

.PHONY: cover
cover:
	go test -cover -coverprofile=coverage.out $$(go list ./... | grep -v cmd)
	go tool cover -o coverage.html -html=coverage.out

.PHONY: $(commands)
$(commands): %: cmd/%.go
	go build -o $@ $<

.PHONY: install
install:
	for cmd in $(commands); do go build -ldflags="-s -w" -o $$cmd cmd/$$cmd.go && sudo install $$cmd /usr/local/bin/$$cmd; done

.PHONY: uninstall
uninstall:
	for cmd in $(commands); do rm -f /usr/local/bin/$$cmd; done

.PHONY: clean
clean: 
	rm -f $(commands)
