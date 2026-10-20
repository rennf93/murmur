.PHONY: build test coverage verify clean

build:
	npm run build

test: build
	npm test

coverage: build
	npm run coverage

verify: build
	node scripts/e2e.mjs

clean:
	rm -rf dist coverage
