
.PHONY: proto
proto:
	@buf generate

.PHONY: mock
mock:
	@bash ./scripts/mockgen.bash
