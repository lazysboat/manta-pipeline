#!/usr/bin/env sh
# manta-pipeline CLI installer (Ubuntu/Linux, amd64 + arm64).
#
#   curl -fsSL https://github.com/lazysboat/manta-pipeline/releases/latest/download/install.sh | sh
#   curl -fsSL .../install.sh | sh -s -- v0.1.0.dev1     # pin a version
#
# Override repo:    MANTA_REPO=owner/repo
# Override target:  INSTALL_DIR=$HOME/.local/bin
set -eu

REPO="${MANTA_REPO:-lazysboat/manta-pipeline}"        # override with MANTA_REPO=owner/repo
VERSION="${1:-latest}"
BIN="manta-pipeline"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

[ "$(uname -s)" = "Linux" ] || { echo "manta-pipeline preview supports Linux/Ubuntu only" >&2; exit 1; }
case "$(uname -m)" in
  x86_64|amd64)   ARCH=amd64 ;;
  aarch64|arm64)  ARCH=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

if [ "$VERSION" = "latest" ]; then
  BASE="https://github.com/$REPO/releases/latest/download"
else
  BASE="https://github.com/$REPO/releases/download/$VERSION"
fi
ASSET="$BIN-linux-$ARCH"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading $ASSET ($VERSION)..."
curl -fsSL "$BASE/$ASSET"      -o "$TMP/$BIN"
curl -fsSL "$BASE/SHA256SUMS"  -o "$TMP/SHA256SUMS"

echo "Verifying checksum..."
EXPECTED="$(grep " $ASSET\$" "$TMP/SHA256SUMS" | awk '{print $1}')"
[ -n "$EXPECTED" ] || { echo "no checksum for $ASSET in SHA256SUMS" >&2; exit 1; }
ACTUAL="$(sha256sum "$TMP/$BIN" | awk '{print $1}')"
[ "$EXPECTED" = "$ACTUAL" ] || { echo "checksum mismatch: expected $EXPECTED, got $ACTUAL" >&2; exit 1; }

chmod +x "$TMP/$BIN"
echo "Installing to $INSTALL_DIR ..."
if [ -w "$INSTALL_DIR" ]; then
  mv "$TMP/$BIN" "$INSTALL_DIR/$BIN"
else
  sudo mv "$TMP/$BIN" "$INSTALL_DIR/$BIN"
fi

echo "Installed: $("$INSTALL_DIR/$BIN" version)"
echo "Next: install 'uv' (https://astral.sh/uv) and run 'manta-pipeline' in a Ray-based project."
