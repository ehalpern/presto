#!/bin/bash
# Restart rather than start to ensure the server is recycled. The ApplicationStop
# hook is called during normal deployment, but it's not called when deploying
# to a newly provisioned instance (via auto-scaling)
sudo service presto-noms-thrift restart
sudo service presto restart
