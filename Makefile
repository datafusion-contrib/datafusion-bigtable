bigtable:
	docker run --rm --name bigtable -d -p 8086:8086 -v $(CURDIR)/script:/opt/script gcr.io/google.com/cloudsdktool/cloud-sdk gcloud beta emulators bigtable start --host-port=0.0.0.0:8086

seed:
	docker exec -e BIGTABLE_EMULATOR_HOST="localhost:8086" bigtable /opt/script/seed.sh

build:
	cargo build

test:
	BIGTABLE_EMULATOR_HOST="localhost:8086" cargo test -- --nocapture

fmt:
	cargo fmt
