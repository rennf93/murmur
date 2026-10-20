.PHONY: build test coverage verify bench-consult clean

build:
	npm run build

test: build
	npm test

coverage: build
	npm run coverage

CLI ?= claude

# One live check against a real CLI: make verify CLI=claude|codex|gemini|hummin|opencode|zcode
verify: build
	node scripts/live/$(CLI).mjs

# Real Claude sessions, six runs: native SendMessage versus murmur. Writes docs/bench/consult.{md,json}.
bench-consult: build
	node scripts/live/bench-consult.mjs

clean:
	rm -rf dist coverage
