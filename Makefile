.PHONY: test
test:
	docker build -t pgtxdb:latest .
	docker run -d --name pgtxdb -p 5432:5432 pgtxdb:latest
	until docker exec pgtxdb pg_isready; do sleep 1; done
	go test -v
	docker stop pgtxdb
	docker rm pgtxdb
