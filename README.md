	
[![Join the chat at https://gitter.im/couchbase/mobile](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/discuss?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![GoDoc](https://godoc.org/github.com/couchbaselabs/sgload?status.png)](https://godoc.org/github.com/couchbaselabs/sgload)

Load testing tool for Sync Gateway intended to allow more flexible scenarios than [gateload](https://github.com/couchbaselabs/gateload) 

![sgload](https://cloud.githubusercontent.com/assets/296876/18367549/8d995bf4-75d0-11e6-8d91-a0056b00346e.png)

## How to run

### Run Sync Gateway

1. Install and run Sync Gateway locally with the [basic-walrus-bucket.json](https://github.com/couchbase/sync_gateway/blob/master/examples/basic-walrus-bucket.json) example config

### Run sgload

```
$ go get -u -v github.com/couchbaselabs/sgload
$ sgload writeload --createusers --numwriters 1 --numdocs 4 --numchannels 4
```

To view more options, run `sgload writeload --help`

## Supported scenarios

* [Write load](https://github.com/couchbaselabs/mobile-testkit/issues/607)

## Design

1. The docfeeder goroutine spreads the docs among the writers as evenly as possible.
1. Docs are spread evenly as possible among channels
1. Can specify existing user credentials or tell the tool to create new users as needed (access to admin port required)
1. When auto-generating users, the user id's will be unique and not interfere with subsequent runs

## Open questions

1. Not sure how many sockets the default httpclient / transport will open??  Want to simulate clients on own devices, so leaning towards more connections
1. Any reason to use multiple http.Clients or http.Transports?
1. Any reason to tweak MaxIdleConnections setting
1. Each writer gets it’s own goroutine.  Should there be a cap on how many can concurrently connect to SG?

## Roadmap

1. Collect metrics
1. Add scenarios


## How to add new command line args

This uses [cobra](https://github.com/spf13/cobra) for managing the CLI user interface, so see the cobra docs for more info.

