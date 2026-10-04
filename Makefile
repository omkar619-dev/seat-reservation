TEST_DATABASE_URL ?= postgres://seats:seats@localhost:55432/seats?sslmode=disable
BASE_URL ?= http://localhost:8080

.PHONY: up down db test test-short build run burst logs

up:            ## run the full stack (Postgres + app) exactly as deployed
	docker compose up -d --build

down:          ## stop the stack and delete local data
	docker compose down -v

db:            ## start only Postgres (for tests / go run)
	docker compose up -d --wait postgres

test: db       ## run all tests, including the Postgres concurrency tests, with -race
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 ./...

test-short:    ## unit tests only (no database)
	go test -count=1 ./...

build:
	CGO_ENABLED=0 go build -o bin/server ./cmd/server
	CGO_ENABLED=0 go build -o bin/burst ./cmd/burst

run: db        ## run the server locally against compose Postgres
	DATABASE_URL='$(TEST_DATABASE_URL)' go run ./cmd/server

burst:         ## on-sale stampede + correctness checks: make burst BASE_URL=https://...
	./burst.sh $(BASE_URL)

logs:
	docker compose logs -f app
