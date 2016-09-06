
Load testing tool for Sync Gateway intended to replace [gateload](https://github.com/couchbaselabs/gateload) -- in the meantime, it will be callable from Gateload.

## How to run

```
go get -u -v github.com/couchbaselabs/sgload
go run main.go
```

## How to add new command line args

This uses [cobra](https://github.com/spf13/cobra) for managing the CLI user interface, so see the cobra docs for more info.

