module github.com/accretional/grpc-server-config

go 1.25.5

replace (
	github.com/accretional/gluon => ../gluon
	github.com/accretional/proto-symbol => ../proto-symbol
)

require (
	github.com/accretional/gluon v0.0.0-20260416101250-f17ade0afc84 // indirect
	github.com/accretional/proto-symbol v0.0.0-20260410152153-06b1dd86f1b8 // indirect
	github.com/accretional/runrpc v0.0.0-20260312135111-3b4543f39b87 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
