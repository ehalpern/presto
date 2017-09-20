#!/bin/bash

. $(dirname $0)/common_functions.sh

ASG=$(get_autoscaling_group)
ROLE=$(role_from_asg $ASG)
DISCOVERY_URI=http://localhost:8080

config=/etc/presto/config.properties

sed -i "s#\(^discovery-uri=\).*#\1=${DISCOVERY_URI}#g" ${config}

if [ "$ROLE" == "master" ]; then
    sed -i 's/\(^coordinator=\).*/\1=true/g' ${config}
    sed -i 's/\(^discovery-server.enabled\)=.*/\1=true/g' ${config}
    sed -i 's/\(^node-scheduler.include-coordinator\)=.*/\1=false/g' ${config}
else
    sed -i 's/\(^coordinator=\).*/\1=false/g' ${config}
    sed -i 's/\(^discovery-server.enabled\)=.*/\1=false/g' ${config}
    sed -i 's/\(^node-scheduler.include-coordinator\)=.*/\1=false/g' ${config}
fi
