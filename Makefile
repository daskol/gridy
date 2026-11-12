.PHONY: generate check-api

generate:
	go tool buf generate
	go tool buf format --diff --exit-code
	go tool buf lint
	go tool buf build
	go test ./pkg/api/...
