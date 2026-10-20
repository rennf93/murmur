.PHONY: build test coverage verify clean

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

clean:
	rm -rf dist coverage
