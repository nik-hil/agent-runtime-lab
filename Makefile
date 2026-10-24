.PHONY: build unit test bench experiments fmt clean
build:
	./scripts/build.sh
unit:
	./scripts/test.sh unit
test:
	./scripts/test.sh integration
bench:
	./scripts/benchmark.sh
experiments:
	./bin/lab experiments --backend all --output results/experiments.json
fmt:
	./scripts/format.sh
clean:
	./scripts/cleanup.sh
