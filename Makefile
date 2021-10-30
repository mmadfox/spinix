.PHONY: docker-protoc
docker-protoc:
	@docker build -t github.com/mmadfox/spinix/protoc:latest -f   \
           ./proto/protoc.dockerfile .

.PHONY: proto
proto: docker-protoc
	@bash ./protoc.bash
