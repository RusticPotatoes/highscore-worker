.PHONY: build up down clean cleanbuild

build:
	docker-compose build

up:
	docker-compose up -d

updev:
	docker-compose -f docker-compose-dev.yml up -d 

down:
	docker-compose down

clean:
	docker-compose down --volumes

cleanbuild: clean
	docker-compose up --build

create:
	python3 -m venv .venv

activate:
	source .venv/bin/activate

requirements:
	pip install -r requirements.txt