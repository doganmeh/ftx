

setup:
	pip3 install pipenv
	pipenv install
	cp template.env .env
	pipenv run python db.py
	echo "Put your API credentials in the `.env` file"
	pipenv shell

reset:
	pipenv run python db.py

run: reset
	python main.py FTX:BTC-PERP

test:
	pytest tests.py

