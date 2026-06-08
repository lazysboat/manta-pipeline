#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Generate gRPC stubs from proto/broker.proto
echo "Generating proto stubs..."

PATH="$PATH:$(go env GOPATH)/bin" protoc \
  --go_out=. --go-grpc_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_opt=paths=source_relative \
  proto/broker.proto
mv proto/broker.pb.go brokerpb/broker.pb.go
mv proto/broker_grpc.pb.go brokerpb/broker_grpc.pb.go

python3 -m grpc_tools.protoc -I proto \
  --python_out=../python-api/mantapipeline/ \
  --grpc_python_out=../python-api/mantapipeline/ \
  proto/broker.proto

# grpc_tools emits a flat `import broker_pb2`; rewrite it to a package-relative
# import so the stubs work when mantapipeline is installed as a package (pip).
sed -i 's/^import broker_pb2 as broker__pb2$/from . import broker_pb2 as broker__pb2/' \
  ../python-api/mantapipeline/broker_pb2_grpc.py

echo "Proto stubs generated."

# Stage Python files for embedding
echo "Staging embed files..."
rm -rf embed/mantapipeline
mkdir -p embed/mantapipeline
cp ../python-api/mantapipeline/*.py embed/mantapipeline/

echo "Embed files staged."

go build -o manta-pipeline .
sudo mv manta-pipeline /usr/local/bin/manta-pipeline

echo "Installed: manta-pipeline -> /usr/local/bin/manta-pipeline"
