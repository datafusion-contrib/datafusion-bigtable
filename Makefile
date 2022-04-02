bigtable:
	docker network create bigtable
	docker run --rm --network=bigtable --name bigtable -d -p 8086:8086 -v $(CURDIR)/script:/opt/script gcr.io/google.com/cloudsdktool/cloud-sdk gcloud beta emulators bigtable start --host-port=0.0.0.0:8086

seed:
	docker exec -e BIGTABLE_EMULATOR_HOST="localhost:8086" bigtable /opt/script/create_table.sh
# 	docker run --rm --network=bigtable -e BIGTABLE_EMULATOR_HOST="bigtable:8086" -v $(CURDIR)/script:/opt/script python:3.9 /bin/bash -c "pip install google-cloud-bigtable==2.4.0 && python /opt/script/insert_rows.py"
# 	docker run --rm -e BIGTABLE_EMULATOR_HOST="host.docker.internal:8086" -v $(CURDIR)/script:/opt/script python:3.9 /bin/bash -c "pip install google-cloud-bigtable==2.4.0 && python /opt/script/insert_rows.py"
	pip install google-cloud-bigtable==2.4.0 && BIGTABLE_EMULATOR_HOST="localhost:8086" python ./script/insert_rows.py

build:
	cargo build

test:
	BIGTABLE_EMULATOR_HOST="localhost:8086" cargo test -- --nocapture

fmt:
	cargo fmt
