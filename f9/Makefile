# Local Makefile for F9All testing
# Run from f9/ directory: make run

.PHONY: run clean

run:
	@echo "Running dbkrab with F9All config..."
	@cd .. && make build
	@cd .. && ./bin/dbkrab -config f9/config.yml

clean:
	@echo "Cleaning local data..."
	@rm -rf data/*.db data/*.json
