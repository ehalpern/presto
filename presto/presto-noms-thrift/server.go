package main

import (
	"log"

	. "prestothriftservice"

	"git.apache.org/thrift.git/lib/go/thrift"
)

func runServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, dbPrefix string) error {
	var transport thrift.TServerTransport
	var err error
	transport, err = thrift.NewTServerSocket(addr)

	if err != nil {
		return err
	}
	handler := &ServiceHandler{
		dbPrefix: dbPrefix,
	}
	processor := NewPrestoThriftServiceProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	log.Printf("Starting thrift server... on %v (%T)", addr, transport)
	return server.Serve()
}
