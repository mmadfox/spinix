FROM golang:1.17 AS builder

ENV PROTOC_VERSION "3.19.1"
ENV PROTOC_GEN_GO_VERSION "1.5.2"
ENV PROTOC_GOGO_VERSION "1.3.2"

RUN apt-get update -yqq && \
  apt-get install -yqq curl git unzip

RUN curl -sfLo protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" && \
  mkdir protoc && \
  unzip -q -d protoc protoc.zip

RUN git clone -q https://github.com/golang/protobuf && \
  cd protobuf && \
  git checkout -q tags/v${PROTOC_GEN_GO_VERSION} -b build && \
  go build -o /go/bin/protoc-gen-go ./protoc-gen-go

RUN git clone -q https://github.com/gogo/protobuf.git protobufgofast  && \
  cd protobufgofast && git checkout -q tags/v${PROTOC_GOGO_VERSION} && \
  go build -o /go/bin/protoc-gen-gofast ./protoc-gen-gofast && \
  go build -o /go/bin/protoc-gen-gogofaster ./protoc-gen-gogofaster && \
  go build -o /go/bin/protoc-gen-gogoslick ./protoc-gen-gogoslick

FROM debian:buster-slim
COPY --from=builder /go/bin/protoc-gen-gogofaster /usr/local/bin/protoc-gen-gogofaster
COPY --from=builder /go/bin/protoc-gen-gogoslick /usr/local/bin/protoc-gen-gogoslick
COPY --from=builder /go/bin/protoc-gen-gofast /usr/local/bin/protoc-gen-gofast
COPY --from=builder /go/protoc/include/google /usr/local/include/google
COPY --from=builder /go/protoc/bin/protoc /usr/local/bin/protoc
COPY --from=builder /go/bin/protoc-gen-go /usr/local/bin/protoc-gen-go
ENTRYPOINT ["/usr/local/bin/protoc"]