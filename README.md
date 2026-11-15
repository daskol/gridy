![On push workflow.](https://github.com/daskol/gridy/actions/workflows/on-push.yml/badge.svg)

# Gridy

*A lightweight library for coordinating hyperparameter search.*

Gridy is a lightweight library for coordinating hyperparameter search. A Go
coordinator distributes trials to Python workers and persists parameters,
progress, and results in a local database.

## Overview

See the [minimal example](examples/README.md) to install Gridy, start a
coordinator, run a Python worker, and export results. Python wheels bundle the
coordinator. Building from source requires Python 3.12+ and the Go toolchain
specified in [go.mod](go.mod).

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
