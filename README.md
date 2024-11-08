# Protobuf to Terraform google_bigquery_table resource

- Install `uv`, `go` and `protobuf`
```
brew install uv go protobuf
```

- Create venv and install python requirements
```
uv venv
uv pip install -r requirements.txt
```

- Add go bin to PATH
```
cat << EOF >> ~/.zshrc
# Add go bin to PATH
export PATH=\$HOME/go/bin:\$PATH
EOF
```

- Install `protoc-gen-bq-schema`
```
go install github.com/GoogleCloudPlatform/protoc-gen-bq-schema@latest
```
