#!/bin/bash

. $(dirname $0)/common_functions.sh

ASG=$(get_autoscaling_group)
ROLE=$(role_from_asg $ASG)
DISCOVERY_URI=http://master.presto.attic.io:8080

config=/etc/presto/config.properties
node=/etc/presto/node.properties

sed -i "s#\(^discovery.uri=\).*#\1${DISCOVERY_URI}#g" ${config}
sed -i 's/\(^node.environment==\).*/\1production/g' ${node}


if [ "$ROLE" == "master" ]; then
    sed -i 's/\(^coordinator=\).*/\1true/g' ${config}
    sed -i 's/\(^discovery-server.enabled=\).*/\1true/g' ${config}
    sed -i 's/\(^node-scheduler.include-coordinator=\).*/\1false/g' ${config}

    masterip=$(host master.presto.attic.io | awk '{ print $4 }')
    aws-ec2-assign-elastic-ip --valid-ips ${masterip}
else
    sed -i 's/\(^coordinator=\).*/\1false/g' ${config}
    sed -i 's/\(^discovery-server.enabled=\).*/\1false/g' ${config}
    sed -i 's/\(^node-scheduler.include-coordinator=\).*/\1false/g' ${config}
fi
