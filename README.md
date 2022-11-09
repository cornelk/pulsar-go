# Apache Pulsar Golang Client Library

[![Build status](https://github.com/cornelk/pulsar-go/actions/workflows/go.yaml/badge.svg?branch=main)](https://github.com/cornelk/pulsar-go/actions)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/cornelk/pulsar-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/cornelk/pulsar-go)](https://goreportcard.com/report/github.com/cornelk/pulsar-go)
[![codecov](https://codecov.io/gh/cornelk/pulsar-go/branch/main/graph/badge.svg?token=NS5UY28V3A)](https://codecov.io/gh/cornelk/pulsar-go)


An alternative Golang client library for the [Apache Pulsar](https://pulsar.apache.org/) project.

## Benefits over other Pulsar Go libraries

* Faster message processing
* Pure Golang, works without use of Cgo
* Idiomatic and cleaner Go
* Better stability
* Allows specifying of initial positions for topic pattern subscriptions
* Higher test coverage
* Pluggable logger interface

## Status

The library is in an early state of development, the API is not stable yet.
Any help or input is welcome.

## Alternative libraries

* [apache/pulsar-client-go](https://github.com/apache/pulsar-client-go)
  the official Golang Client that inspired the creation of this alternative Client.

* [apache/pulsar/pulsar-client-go](https://github.com/apache/pulsar-client-go)
  Cgo based Client library that will be deprecated.

* [Comcast/pulsar-client-go](https://github.com/Comcast/pulsar-client-go)
  an older Client that appears to not be maintained anymore and lacking features like Batching.
