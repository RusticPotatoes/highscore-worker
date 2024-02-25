.PHONY: build up down clean cleanbuild

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

clean:
	docker-compose down --volumes

cleanbuild: clean
	docker-compose up --build