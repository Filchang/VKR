up:
	docker-compose up -d

down:
	docker-compose down

test:
	docker-compose exec app pytest tests/ -v

logs:
	docker-compose logs -f app

shell:
	docker-compose exec app bash

migrate:
	docker-compose exec app alembic upgrade head
