# Contributing to murmur

Murmur is a small local message bus. Keep the core small: a feature that one CLI's adapter can own belongs in that adapter, not in the broker.

## The one rule

You must understand your code. If you cannot explain what a change does and how it interacts with the rest of the system, the PR will be closed. Using AI to write code is fine; submitting AI-generated code you do not understand is not.

## Setup

Requires Node 22.12 or newer (vitest 5 needs it).

```
npm install
npm run build
npm test
```

## Before a PR

- `npm run build` compiles with strict TypeScript.
- `npm run coverage` must stay at 100 percent statements, branches, functions and lines. A new branch needs a test.
- Behavior that touches a real CLI should be checked with `make verify CLI=<cli>` (see the Verification page of the docs); it spends a few cents of model calls.
- Docs under `docs/` and `README.md` change with the behavior they describe. Every fact there must come from the code.
- Add an entry to `CHANGELOG.md` for user-visible changes.
- Fill in the pull request template: what changed, the issue, how you verified it, and any behavior change.
- Keep commits focused, with messages that say what changed and why.

## Docs

The site is mkdocs-material. To preview locally in a throwaway environment:

```
uvx --with mkdocs-material mkdocs serve
```

`mkdocs build --strict` must pass. The Docs CI workflow also runs lychee (links, external included), markdownlint-cli2 (`.markdownlint-cli2.yaml`) and typos (`_typos.toml`); run them locally with `npx markdownlint-cli2`, `lychee --config lychee.toml README.md 'docs/**/*.md'` and `typos`.

## Issues

Use the issue forms. Keep them short and concrete: what happened, what you expected, the CLI and version, and `murmur status` output if relevant. For security reports, see [SECURITY.md](SECURITY.md) and do not open a public issue. Questions go through [SUPPORT.md](SUPPORT.md).

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).
