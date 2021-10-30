#!/bin/bash
ROOT="$(pwd)"
docker run --rm -v $ROOT:$ROOT -w $ROOT github.com/mmadfox/spinix/protoc --proto_path=. --gogoslick_out=. -I . ./proto/spinix.proto