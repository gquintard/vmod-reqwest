#!/bin/sh

set -ex

OUT="$1"

if [ -z "$OUT" ]; then
	OUT=.
fi

if [ -z "$PKG" ]; then
	PKG="$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].name')"
fi

if [ -z "" ]; then
	VMODTOOL="$(pkg-config  --variable=vmodtool varnishapi)"
fi

cargo build --release
cargo test --release

mkdir -p "$OUT"
cp target/debug/lib$PKG.so "$OUT"
rst2man $PKG.man.rst > "$OUT/$PKG.3"
"$VMODTOOL" vmod.vcc -w "$OUT" --output /tmp/tmp_file_to_delete
rm /tmp/tmp_file_to_delete.*
