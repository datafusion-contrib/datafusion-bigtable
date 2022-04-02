install:
	pipenv install

check:
	cargo check

dev_build:
	pipenv run maturin develop

build:
	pipenv run maturin build

test:
	BIGTABLE_EMULATOR_HOST="localhost:8086" pipenv run pytest datafusion_bigtable/tests

fmt:
	cargo fmt
	pipenv run black .
