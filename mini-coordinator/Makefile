.PHONY: gen # tells Make that gen isnt an actul file and might not exist
# since it will be generated after the command finishes
gen:
	protoc \
	  --go_out=./gen \
	  --go-grpc_out=./gen \
	  proto/coord.proto proto/ps.proto

tidy:
	go mod tidy
