run: build
	@./bin/gofka

build: 
	@go build -o bin/gofka .