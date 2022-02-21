install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest

format:
	autopep8 --in-place --aggressive --aggressive src/*.py

lint:
	pylint --disable=R,C src/*.py