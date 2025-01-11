# Protobuf to Terraform google_bigquery_table resource

## Pre-requisites
- Install `uv` then create venv and install python requirements
```
brew install uv
uv venv
uv pip install -r requirements.txt
chown -R $(id -u):$(id -g) .venv
```

- Download binary of `protoc` and `protoc-gen-bq-schema` to `.venv/bin`
```
# protoc
PB_VERSION=28.3
wget -O /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v$PB_VERSION/protoc-$PB_VERSION-osx-aarch_64.zip
unzip /tmp/protoc.zip -d /tmp/protoc
cp -r /tmp/protoc/bin/ .venv/bin/
cp -r /tmp/protoc/include/ .venv/include/

# protoc-gen-bq-schema
GBS_VERSION=1.1.0
wget -O /tmp/protoc-gen-bq-schema https://github.com/GoogleCloudPlatform/protoc-gen-bq-schema/releases/download/v$GBS_VERSION/protoc-gen-bq-schema_darwin_arm64
chmod +x /tmp/protoc-gen-bq-schema
cp -r /tmp/protoc-gen-bq-schema .venv/bin/
```

## Instructions
```
CONTROL_PLANE_DIR=
PUBSUB_SCHEMA_NAME=
BIGQUERY_TABLE_NAME=
PARTITIONING_FIELD=
uv run python protobuf_to_bq_schema.py $CONTROL_PLANE_DIR $PUBSUB_SCHEMA_NAME $BIGQUERY_TABLE_NAME --partitioning_field $PARTITIONING_FIELD
```
