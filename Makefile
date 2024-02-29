.PHONY: build up down clean cleanbuild

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

docker-restart:
	docker compose down
	docker compose up --build -d

clean:
	docker-compose down --volumes

cleanbuild: clean
	docker-compose up --build

updev: clean
	docker-compose -f docker-compose-dev.yml up -d 

create:
	python3 -m venv .venv

activate:
	source .venv/bin/activate

requirements:
	pip install -r requirements.txt