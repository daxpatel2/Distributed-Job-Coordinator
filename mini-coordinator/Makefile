.PHONY: gen # tells Make that gen isnt an actul file and might not exist
# since it will be generated after the command finishes
gen:
	protoc \
	  --go_out=./gen --go_opt=paths=source_relative \
	  --go-grpc_out=./gen --go-grpc_opt=paths=source_relative \
	  proto/coord.proto