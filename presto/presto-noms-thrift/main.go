/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//go:generate thrift -r -out vendor --gen go PrestoThriftService.thrift
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/database"
)

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

func main() {
	flag.Usage = Usage
	protocol := flag.String("P", "binary", "Specify the protocol (binary, compact, json, simplejson)")
	buffered := flag.Bool("buffered", false, "Use buffered transport")
	addr := flag.String("addr", "localhost:9090", "Address to listen to")
	flag.StringVar(&config.dbPrefix,"db-prefix", config.dbPrefix, "Database path prefix")
	flag.StringVar(&config.dbPreloadList,"db-preload-list", config.dbPreloadList, "Databases to preload (comma separated)")
	flag.Uint64Var(&config.workerCount,"worker-count", config.workerCount, "Number of worker nodes")
	flag.Uint64Var(&config.splitsPerWorker,"splits-per-worker", config.splitsPerWorker, "Number of splits/worker")

	database.RegisterDatabaseFlags(flag.CommandLine)
	flag.Parse()
	var protocolFactory thrift.TProtocolFactory
	switch *protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	default:
		fmt.Fprint(os.Stderr, "Invalid protocol specified", protocol, "\n")
		Usage()
		os.Exit(1)
	}

	var transportFactory thrift.TTransportFactory
	if *buffered {
		transportFactory = thrift.NewTBufferedTransportFactory(8192)
	} else {
		transportFactory = thrift.NewTTransportFactory()
	}

	transportFactory = thrift.NewTFramedTransportFactoryMaxLength(transportFactory, 67108864)

	if config.dbPreloadList != "" {
		for _, name := range strings.Split(config.dbPreloadList, ",") {
			fullName := config.dbPrefix + "/" + name
			err := database.Preload(fullName)
			if err != nil {
				log.Printf("Warning: bad db name %s in db-preload-list: %s", fullName, err)
				continue
			}
		}
	}
	log.Printf("config: %+v", config)
	if err := runServer(transportFactory, protocolFactory, *addr, config.dbPrefix); err != nil {
		fmt.Println("error running server:", err)
	}
}
