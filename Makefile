
test:
	go test ./counterd/

build:
	go build -o bin/counterd ./counterd/

build-linux:
	cd counterd/; GOOS=linux GOARCH=amd64 go build -o ../bin/counterd .

pg:
	docker run -p 5432:5432 -d postgres:9.6

redis:
	docker run -p 6379:6379 -d redis

psql:
	psql -h 127.0.0.1 -p 5432 -U postgres

integ:
	INTEG=yes REDIS_ADDR="127.0.0.1:6379" PG_ADDR="postgres://postgres@localhost/postgres?sslmode=disable" go test -v ./counterd/ -timeout=1s
