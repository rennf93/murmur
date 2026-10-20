# Releasing

Releases are cut from a tag. `.github/workflows/release.yml` runs on tags `v*`.

## Flow

1. Update `version` in `package.json` and add a `## [x.y.z] - date` section to `CHANGELOG.md`.
2. Merge to `master`, then tag and push: `git tag vx.y.z && git push origin vx.y.z`.
3. The workflow runs, in order:
    1. The same checks as CI (build and `npm run coverage` on ubuntu and macOS, Node 22 and 24).
    2. A check that fails the run if `package.json` version differs from the tag.
    3. Build, then `npm pack --dry-run` to log the tarball contents.
    4. `npm publish` of the packed tarball with `--provenance`, through npm trusted publishing (GitHub OIDC, no token).
    5. `gh release create` for the tag with notes from that version's `CHANGELOG.md` section and the `.tgz` attached.

A tag with a prerelease suffix (for example `v1.2.3-rc.1`) publishes with `--tag next` and creates a GitHub prerelease.

The package name is read from `package.json`; nothing in the workflows hard-codes it.

## One-time setup

1. **First publish.** The package is `murmur-it` (`murmur` on npm belongs to an unrelated package). npm attaches a trusted publisher only to a package that exists, so publish the first version by hand from a clean checkout of the tag: `npm login`, `npm ci && npm run build`, `npm publish --access public`.
2. **Trusted publisher.** On npmjs.com, open the package, then Settings, then Trusted Publisher, then GitHub Actions, and register: user `rennf93`, repository `murmur`, workflow filename `release.yml`, environment empty.
3. **Repository variable.** Set `MURMUR_TRUSTED_PUBLISH` to `true` under Settings, Secrets and variables, Actions, Variables. Until then a tag still gets its GitHub release (with the packed tarball attached); only the npm publish step is skipped.
4. **GitHub Pages.** The docs workflow publishes the `gh-pages` branch. Under Settings, Pages, set the source to Deploy from a branch: `gh-pages`, `/ (root)`.
