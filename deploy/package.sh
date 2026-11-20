#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${RADIO_VERSION:-0.1.0}"
STAMP="$(date +%Y%m%d-%H%M)"
OUT="$ROOT/dist"
mkdir -p "$OUT"

build() {
  local os="$1" arch="$2" ext="$3"
  local dir="$OUT/radio-go-${VERSION}-${STAMP}-${os}-${arch}"
  mkdir -p "$dir/bin"
  (cd "$ROOT" && GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o "$dir/bin/radio-station-go${ext}" ./cmd/station)
  (cd "$ROOT" && GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o "$dir/bin/radio-relay${ext}" ./cmd/relay)
  (cd "$ROOT" && GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o "$dir/bin/radio-relay-agent${ext}" ./cmd/relay-agent)
  (cd "$ROOT" && GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o "$dir/bin/radio-directory${ext}" ./cmd/directory)
  cp -a "$ROOT/web" "$dir/web"
  if [ -d "$ROOT/plugins" ]; then cp -a "$ROOT/plugins" "$dir/plugins"; fi
  cp "$ROOT/go.mod" "$ROOT/go.sum" "$ROOT/README.md" "$ROOT/LICENSE" "$dir/"
  (cd "$OUT" && tar -czf "radio-go-${VERSION}-${STAMP}-${os}-${arch}.tar.gz" "radio-go-${VERSION}-${STAMP}-${os}-${arch}")
  rm -rf "$dir"
  echo "built radio-go-${VERSION}-${STAMP}-${os}-${arch}.tar.gz"
}

build linux amd64 ""
build linux arm64 ""
build darwin arm64 ""
build darwin amd64 ""
