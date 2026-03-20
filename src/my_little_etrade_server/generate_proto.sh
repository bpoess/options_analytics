#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/proto"
OUT_DIR="$SCRIPT_DIR/generated"

SITE_PACKAGES="$(python -c "import sysconfig; print(sysconfig.get_path('purelib'))")"

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    -I "$SITE_PACKAGES" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    "$PROTO_DIR/my_little_etrade_server.proto"

# Fix generated grpc import to use relative import (grpc_tools generates absolute imports)
sed -i '' 's/^import my_little_etrade_server_pb2/from my_little_etrade_server.generated import my_little_etrade_server_pb2/' \
    "$OUT_DIR/my_little_etrade_server_pb2_grpc.py"

echo "Generated protobuf code in $OUT_DIR"
