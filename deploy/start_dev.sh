#!/usr/bin/env bash
#
# Usage: ./start.sh [service]
# Description: Start the service
# Options:
#   service: The service to start (start the full stack if not provided)
#            e.g. ./start.sh models_model1
#                 will run the models_model1 service

set -eoux pipefail

# Check if the option 'service' variable is set
if [ -n "${1+set}" ]; then
    SERVICE=$1
else
    SERVICE=""
fi

# Get the directory of the script (regardless of where it is called from)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}

# Set PATH to docker and docker-compose (required when running as a cron job)
export PATH=$PATH:/usr/local/bin

# Change to the docker-compose directory and start the service
pushd service
docker-compose --project-name dart up --detach --build $SERVICE
popd
