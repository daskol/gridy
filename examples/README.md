# Examples

## Minimal

This example runs four trials with a dummy objective that returns a random
score. Run the commands from the repository root.

### Install

Building from source requires Python 3.12+, the Go toolchain specified in
[go.mod](../go.mod), and access to their package dependencies.

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install .
```

The installation bundles the Go coordinator with the Python package.

### Start the Coordinator

[search-space.toml](search-space.toml) defines two parameters with two candidate
values each. Gridy runs their Cartesian product as four trials. The file name
sets the sweep identifier: `search-space`.

```sh
gridy serve examples/search-space.toml
```

The coordinator listens on `127.0.0.1:8080` for HTTP and `127.0.0.1:8081` for
gRPC, and saves state in `gridy.db`.

### Run a Worker

In another terminal, activate the same environment and run
[run.multi.py](run.multi.py):

```sh
. .venv/bin/activate
python examples/run.multi.py
```

The worker prints each score and its parameters, then exits when the sweep is
complete. Replace `objective(ctx, params)` in the script with your own function
returning a dictionary of metrics. Start more workers with the same command to
share the trials.

## Export Results

With the coordinator still running, export the completed trials as JSON Lines
as follows.

```sh
gridy export http://127.0.0.1:8080 results.jsonl --sweep search-space
```

Each record contains `trial_id`, `value` (the `objective` metric), and
`params`. Stop the coordinator with Ctrl+C. Restarting with the same search
space and database preserves progress. To change the search space in an
existing database, use a new TOML file name and update the worker's sweep
identifier to match.
