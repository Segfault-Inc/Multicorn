test:
	python tests.py

coverage:
	python -m coverage run tests.py && python -m coverage report -m

