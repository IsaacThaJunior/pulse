# Maintaining pulse

Personal checklist for pushing to `main` and cutting releases. Not for
contributors — see [CONTRIBUTING.md](CONTRIBUTING.md) for that.

## Before every push to main

1. `gofmt -l .` — must print nothing. CI fails the build otherwise; catching
   it locally is faster than waiting on a run.
2. `go vet ./...`
3. `go build ./...`
4. `go test ./... -race` (needs Docker locally for the `redisqueue`
   integration test — CI always runs this regardless, but don't rely on CI
   to be the first check).
5. If you touched `README.md` code samples, actually compile them
   (`go build` a throwaway file with the snippet) — a broken quickstart is
   worse than no quickstart.

None of this is optional-if-in-a-hurry. This is a library; a broken `main`
breaks anyone who happens to `go get` at the wrong moment.

## When to tag a new version

Pre-1.0, so no strict compatibility promise, but tag with intent, not on
every commit:

- **Patch** (`v0.1.1`): bug fixes, doc-only changes, no API surface change.
- **Minor** (`v0.2.0`): new exported functionality (a new package, a new
  method, a new option) — anything additive.
- Breaking a signature pre-1.0 is allowed by semver but still bump minor
  and call it out explicitly in the tag message — don't make someone
  discover it from a compile error.

Tag after a push you've already verified with the checklist above, not
before.

## Cutting a release

```bash
git tag -a v0.X.Y -m "short summary of what changed and why"
git push origin v0.X.Y
```

Then confirm it actually resolves before considering it done:

```bash
go list -m github.com/isaacthajunior/pulse@v0.X.Y
```

## On automating this

I choose not to automated this yet — release-on-merge / conventional-commit
tagging (e.g. via `goreleaser` or a tag-on-push Action) might be the obvious next
step, but that is premature for a single-maintainer, pre-1.0 library with no
release cadence yet. 