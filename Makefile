.PHONY: build test

build:
	@:

test:
	cd 6.5840/src/main; python3 LogVelidation.py

docs:
	cd 6.5840/src/mr; go doc -u -all > mr-doc.txt