#!/usr/bin/env sh
set -eu

APP_NAME="${APP_NAME:-coffee_server}"
GOOS="${GOOS:-linux}"
GOARCH="${GOARCH:-amd64}"
CGO_ENABLED="${CGO_ENABLED:-0}"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
BIN_DIR="${SCRIPT_DIR}/bin"
BACKUP_DIR="${BIN_DIR}/backups"
TARGET="${BIN_DIR}/${APP_NAME}"
LEGACY_TARGET="${BIN_DIR}/coffee-ws-server"
BACKUP_SUFFIX="$(date +%Y%m%d%H%M%S)"

mkdir -p "$BIN_DIR" "$BACKUP_DIR"

if [ -f "$TARGET" ]; then
    BACKUP_TARGET="${BACKUP_DIR}/${APP_NAME}.${BACKUP_SUFFIX}.bak"
    cp -a "$TARGET" "$BACKUP_TARGET"
    echo "Backed up existing binary to ${BACKUP_TARGET}"
fi

if [ -f "$LEGACY_TARGET" ]; then
    LEGACY_BACKUP_TARGET="${BACKUP_DIR}/coffee-ws-server.${BACKUP_SUFFIX}.bak"
    mv "$LEGACY_TARGET" "$LEGACY_BACKUP_TARGET"
    echo "Moved legacy binary to ${LEGACY_BACKUP_TARGET}"
fi

echo "Building ${APP_NAME} for ${GOOS}/${GOARCH}"
CGO_ENABLED="$CGO_ENABLED" GOOS="$GOOS" GOARCH="$GOARCH" go build -o "$TARGET" .
echo "Built ${TARGET}"
