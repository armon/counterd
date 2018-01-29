
test:
	go test ./counterd/

build:
	go build -o bin/counterd ./counterd/

pg:
	docker run -p 5432:5432 -d postgres:9.6

psql:
	psql -h 127.0.0.1 -p 5432 -U postgres

