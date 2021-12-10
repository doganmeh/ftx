

setup:
	pip3 install pipenv
	pipenv install
	pipenv run python db.py
	echo "Put your API credentials in the `.env` file"
	pipenv shell

reset:
	pipenv run python db.py

run:
	python main.py FTX:BTC-PERP

test:
	pytest tests.py

