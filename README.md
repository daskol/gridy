# Gridy

![On push workflow.](https://github.com/daskol/gridy/actions/workflows/on-push.yml/badge.svg)

*Better hyperparameter searcher*

## Overview

This repository and document is created with the purpose to collect functional
and implentation requirements for a client library and server-side service for
hyperparameter optimization.

## Functional Requirements

1. Gridy must work on network file systems and NFS specifically.
2. Gridy must allow to update dynamically search space without creating new
   study (opposed to Optuna).
3. Gridy must provide fail-over mechanism. For example. workers must wait until
   budgest is exhausted or all workers fail if one worker failed.
4. Gridy must allow experiment annotation and editting.
5. Gridy must provide better prunning mechanisms (cf. @evfro).
6. Gridy must provide basic experiment management (cf. @daskol).
7. Gridy must support seeds or other common determenistic hyperparameter out of
   the box.
8. Gridy must provide persistent zero-conf storage as a default experiment
   storage facility.

## Running the server

Python wheels include the Go coordinator in `gridy/bin/gridy`. After installing
a wheel, `gridy serve` runs this bundled executable. Set `GRIDY_BINARY` to use
another executable; source checkouts also look for a Go `gridy` on `PATH`.

```sh
go run ./cmd/gridy serve examples/search-space1.toml \
    --dsn gridy.db \
    --grpc-listen 127.0.0.1:8081 \
    --http-listen 127.0.0.1:8080
```

Pass one or more TOML files to load independent sweeps as follows.

```sh
gridy serve search-space1.toml search-space2.toml
```

Each sweep takes its identifier from the file name without `.toml`. Identifiers
may contain ASCII letters, digits, `_`, and `-`. Files with the same identifier
cannot be loaded together, even from different directories.

Tables preserve the nesting of parameters; each parameter is a nonempty array
of candidate numbers, strings, or booleans. Empty tables, duplicate candidate
values, nonfinite numbers, and compound candidate values are rejected.

```toml
[dataset]
window_size = [3, 5, 7, 9, 11]
num_negatives = [3, 5, 7]

[model]
embed_size = [128, 256, 512]

[optimizer]
batch_size = [512, 1024, 2048, 4096]
lr = [5e-3, 1e-3, 5e-4]
```

The coordinator generates the Cartesian product: this example creates 540
trials. A worker receives one combination as a nested object.

```json
{
  "dataset": {"window_size": 3, "num_negatives": 3},
  "model": {"embed_size": 128},
  "optimizer": {"batch_size": 512, "lr": 0.005}
}
```

State is persisted in the sweep file (default: `gridy.db`).
Restarting with the same file contents resumes existing trials and results.
Comments, whitespace, key order, and equivalent numeric spellings do not change
the search space. Candidate order is significant. Changing a parameter, a
candidate value, or candidate order for an existing sweep fails startup. Use a
new file name to create a different sweep. All input files are validated before
import, and all sweeps are imported in one transaction.

The HTTP listener serves the generated HTTP/JSON API. The Sweep MCP endpoint
at `/mcp` and health endpoint is `/healthz`.

Either listener can use a Unix socket instead of TCP as follows.

```sh
go run ./cmd/gridy serve examples/search-space.toml \
  --grpc-listen unix:///tmp/gridy-grpc.sock \
  --http-listen unix:///tmp/gridy-http.sock
curl --unix-socket /tmp/gridy-http.sock http://localhost/healthz
```

## Python Distributions

Building from source requires the Go toolchain specified in `go.mod` and access
to its module dependencies. Generate an sdist and a wheel with

```sh
python -m pip install build
python -m build
```

The wheel build generates the Go API and compiles the coordinator with CGO
disabled. The resulting wheel has a `py3-none-<platform>` tag for the target
platform and includes the executable with its execution permissions.

Set `GOOS` (`linux` or `darwin`) and `GOARCH` (`amd64` or `arm64`) to
cross-compile. For example, build an Apple Silicon wheel from Linux with

```sh
GOOS=darwin GOARCH=arm64 python -m build --wheel
```

Code generators always run for the build host. The wheel's platform tag follows
the Go target, independently of the Python interpreter running the build.
Darwin wheels require macOS 13 or later, matching the Go 1.27 runtime.
