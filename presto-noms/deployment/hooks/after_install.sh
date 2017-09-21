#!/bin/bash

. $(dirname $0)/common_functions.sh

instance_id=$(get_instance_id)
node=/etc/presto/node.properties

sed -i "s/\(^node.id=\).*/\1${instance_id}/g" ${node}
sed -i 's/\(^node.environment=\).*/\1production/g' ${node}

asg=$(get_autoscaling_group)
role=$(role_from_asg $asg)
config=/etc/presto/config.properties

if [ "$role" == "master" ]; then
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
