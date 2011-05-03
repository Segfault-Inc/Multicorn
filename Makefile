test:
	python queries.py

coverage:
	python -m coverage run queries.py && python -m coverage report -m

