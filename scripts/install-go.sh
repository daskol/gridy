#!/bin/sh
# Install the module's Go toolchain inside cibuildwheel's Linux container.
set -eu

go_version=$(sed -n 's/^go /go/p' "$(dirname "$0")/../go.mod")
case $(uname -m) in
    x86_64) go_arch=amd64 ;;
    aarch64) go_arch=arm64 ;;
    *) echo 'Unsupported Go host architecture' >&2; exit 1 ;;
esac

archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
curl --fail --location --retry 3 \
    "https://go.dev/dl/${go_version}.linux-${go_arch}.tar.gz" -o "$archive"
mkdir /usr/local/go
tar -xzf "$archive" -C /usr/local/go --strip-components=1
