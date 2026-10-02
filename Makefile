.PHONY: help up down logs clean

help:
	@echo "make up - Start the services"
	@echo "make down - Stop the services"
	@echo "make logs - tail logs"
	@echo "make clean - remove all containers and volumes"

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

logs:
	docker-compose -f docker/docker-compose.yml logs -f

clean:
	docker-compose -f docker/docker-compose.yml down -v