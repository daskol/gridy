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

```sh
go run ./cmd/gridy serve \
    --grpc-listen 127.0.0.1:8081 \
    --http-listen 127.0.0.1:8080
```

The HTTP listener serves the generated HTTP/JSON API. The Sweep MCP endpoint
at `/mcp` and health endpoint is `/healthz`.

Either listener can use a Unix socket instead of TCP as follows.

```sh
go run ./cmd/gridy serve \
  --grpc-listen unix:///tmp/gridy-grpc.sock \
  --http-listen unix:///tmp/gridy-http.sock
curl --unix-socket /tmp/gridy-http.sock http://localhost/healthz
```
