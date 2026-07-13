# Contributing to pulse

Contributions are welcome — bug reports, fixes, new queue/store backends,
documentation. This project is young (`v0.1.0`), so there's real room to
shape it.

## Before you start

Read the README's [Why](README.md#why) and
[What pulse deliberately doesn't do](README.md#what-pulse-deliberately-doesnt-do)
sections first. The short version: `pulse` stays small on purpose. If a
change adds a concept the core (`queue.Queue`, `worker.Pool`,
`worker.Store`, `worker.HandlerFunc`) doesn't strictly need — task chaining,
a specific database, an HTTP layer — it probably belongs in your own app
instead, not in this library. If you're not sure whether an idea fits,
open an issue before writing code.

## Development setup

```bash
git clone https://github.com/IsaacThaJunior/pulse
cd pulse
go build ./...
go vet ./...
go test ./... -short   # skips the Redis integration test
go test ./... -race    # full suite; needs Docker for queue/redisqueue's testcontainers test
```

`go run ./examples/simple` runs a working example with no external infra.

## Making a change

1. Fork, branch, make your change.
2. `gofmt -w` your files — CI rejects unformatted code.
3. Add or update tests. `worker/pool_test.go` has the pattern for pool
   behavior (in-memory fakes for `queue.Queue`/`worker.Store`);
   `worker/concurrency_test.go` is the pattern for anything touching
   concurrent access.
4. Open a PR. CI (`gofmt`, `go vet`, `go build`, `go test -race`) must pass.

## Reporting bugs

Open an issue with what you expected, what happened, and — if you can — a
minimal reproduction. If it's a race or a concurrency bug, note whether
`-race` catches it.

## License

By contributing, you agree your contribution is licensed under this
project's [MIT license](LICENSE).
