#!/bin/bash
#
# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

# TARGET_LIST defines which target groups behind Load Balancer this instance should be part of.
# The elements in TARGET_LIST should be seperated by space.
# Not needed if using autoscaling
TARGET_GROUP_LIST=""

# PORT defines which port the application is running at.
# If PORT is not specified, the script will use the default port set in target groups
PORT=""

# Under normal circumstances, you shouldn't need to change anything below this line.
# -----------------------------------------------------------------------------

export PATH="$PATH:/usr/bin:/usr/local/bin"

# If true, all messages will be printed. If false, only fatal errors are printed.
DEBUG=true

# If true, all commands will have a initial jitter - use this if deploying to significant number of instances only
INITIAL_JITTER=false

# Number of times to check for a resouce to be in the desired state.
WAITER_ATTEMPTS=60

# Number of times to check for a resouce to be in the desired state when putting a host in an ASG
# into StandBy. By default, ELB application load balancers wait 300 seconds for connections to drain,
# so this will wait 360 seconds before timing out. To reduce or increase the timeout, increase/decrease
# the connection draining in your ELB accordingly and update this value.
WAITER_ATTEMPTS_ASG_ENTER_STANDBY=120

# Number of seconds to wait between attempts for resource to be in a state for instance in ASG.
WAITER_INTERVAL_ASG=3

# Number of seconds to wait between attempts for resource to be in a state for ALB registration/deregistration.
WAITER_INTERVAL_ALB=10

# AutoScaling Standby features at minimum require this version to work.
MIN_CLI_VERSION='1.10.55'

#
# Performs CLI command and provides expotential backoff with Jitter between any failed CLI commands
# FullJitter algorithm taken from: https://www.awsarchitectureblog.com/2015/03/backoff.html
# Optional pre-jitter can be enabled  via GLOBAL var INITIAL_JITTER (set to "true" to enable)
#
exec_with_fulljitter_retry() {
    local MAX_RETRIES=${EXPBACKOFF_MAX_RETRIES:-8} # Max number of retries
    local BASE=${EXPBACKOFF_BASE:-2} # Base value for backoff calculation
    local MAX=${EXPBACKOFF_MAX:-120} # Max value for backoff calculation
    local FAILURES=0
    local RESP

    # Perform initial jitter sleep if enabled
    if [ "$INITIAL_JITTER" = "true" ]; then
      local SECONDS=$(( $RANDOM % ( ($BASE * 2) ** 2 ) ))
      sleep $SECONDS
    fi

    # Execute Provided Command
    RESP=$(eval $@)
    until [ $? -eq 0 ]; do
        FAILURES=$(( $FAILURES + 1 ))
        if (( $FAILURES > $MAX_RETRIES )); then
            echo "$@" >&2
            echo " * Failed, max retries exceeded" >&2
            return 1
        else
            local SECONDS=$(( $RANDOM % ( ($BASE * 2) ** $FAILURES ) ))
            if (( $SECONDS > $MAX )); then
                SECONDS=$MAX
            fi

            echo "$@" >&2
            echo " * $FAILURES failure(s), retrying in $SECONDS second(s)" >&2
            sleep $SECONDS

            # Re-Execute provided command
            RESP=$(eval $@)
        fi
    done

    # Echo out CLI response which is captured by calling function
    echo $RESP
    return 0
}

# Usage: get_instance_region
#
#   Writes to STDOUT the AWS region as known by the local instance.
get_instance_region() {
    if [ -z "$AWS_REGION" ]; then
        AWS_REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document \
            | grep -i region \
            | awk -F\" '{print $4}')
    fi

    echo $AWS_REGION
}

AWS_CLI="exec_with_fulljitter_retry aws --region $(get_instance_region)"

# Usage: autoscaling_group_name <EC2 instance ID>
#
#    Prints to STDOUT the name of the AutoScaling group this instance is a part of and returns 0. If
#    it is not part of any groups, then it prints nothing. On error calling autoscaling, returns
#    non-zero.
autoscaling_group_name() {
    local instance_id=$1

    # This operates under the assumption that instances are only ever part of a single ASG.
    local autoscaling_name=$($AWS_CLI autoscaling describe-auto-scaling-instances \
        --instance-ids $instance_id \
        --output text \
        --query AutoScalingInstances[0].AutoScalingGroupName)

    if [ $? != 0 ]; then
        return 1
    elif [ "$autoscaling_name" == "None" ]; then
        echo ""
    else
        echo "${autoscaling_name}"
    fi

    return 0
}

# Usage: check_cli_version [version-to-check] [desired version]
#
#   Without any arguments, checks that the installed version of the AWS CLI is at least at version
#   $MIN_CLI_VERSION. Returns non-zero if the version is not high enough.
check_cli_version() {
    if [ -z $1 ]; then
        version=$($AWS_CLI --version 2>&1 | cut -f1 -d' ' | cut -f2 -d/)
    else
        version=$1
    fi

    if [ -z "$2" ]; then
        min_version=$MIN_CLI_VERSION
    else
        min_version=$2
    fi

    x=$(echo $version | cut -f1 -d.)
    y=$(echo $version | cut -f2 -d.)
    z=$(echo $version | cut -f3 -d.)

    min_x=$(echo $min_version | cut -f1 -d.)
    min_y=$(echo $min_version | cut -f2 -d.)
    min_z=$(echo $min_version | cut -f3 -d.)

    msg "Checking minimum required CLI version (${min_version}) against installed version ($version)"

    if [ $x -lt $min_x ]; then
        return 1
    elif [ $y -lt $min_y ]; then
        return 1
    elif [ $y -gt $min_y ]; then
        return 0
    elif [ $z -ge $min_z ]; then
        return 0
    else
        return 1
    fi
}

# Usage: msg <message>
#
#   Writes <message> to STDERR only if $DEBUG is true, otherwise has no effect.
msg() {
    local message=$1
    $DEBUG && echo $message 1>&2
}

# Usage: error_exit <message>
#
#   Writes <message> to STDERR as a "fatal" and immediately exits the currently running script.
error_exit() {
    local message=$1

    echo "[FATAL] $message" 1>&2
    exit 1
}

# Usage: get_instance_id
#
#   Writes to STDOUT the EC2 instance ID for the local instance. Returns non-zero if the local
#   instance metadata URL is inaccessible.
get_instance_id() {
    curl -s http://169.254.169.254/latest/meta-data/instance-id
    return $?
}

# Usage: get_autoscaling_group
#
#   Writes the auto-scaling group to STDOUT.
#   Returns non-zero if the local instance is not in a an auto-scaling group.
get_autoscaling_group() {
    local instance=$(get_instance_id)
    if [ $? != 0 ]; then
        return $?
    fi
    local group=$(autoscaling_group_name $instance)
    if [ $? != 0 -o -z "${group}" ]; then
        return $?
    fi
    echo $group
}

# Usage: role_from_asg <autoscaling_group>
#
role_from_asg() {
    local group=$1
    group_array=(${group//-/ })
    if [[ "${#group_array[@]}" != 3 ]]
    then
        msg "Auto scaling group ${group} should have the form <role>-presto-group"
        return 1
   fi
   echo "${group_array[0]}"
}

