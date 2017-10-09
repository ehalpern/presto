#!/bin/bash
set -e
sudo service presto status | grep Running
sudo service presto-noms-thrift status | grep Running
