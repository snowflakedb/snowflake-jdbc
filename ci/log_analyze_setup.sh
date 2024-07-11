#!/bin/bash -e
#
# preparation for log analyze
#

# Note, this need to be consistent with docker bind parameter. Might need to come up with a better sync up solution
LOCAL_CLIENT_LOG_DIR_PATH_DOCKER=/mnt/workspace/jenkins_rt_logs
LOCAL_CLIENT_LOG_DIR_PATH=$WORKSPACE/jenkins_rt_logs
echo "[INFO] LOCAL_CLIENT_LOG_DIR_PATH=$LOCAL_CLIENT_LOG_DIR_PATH"
echo "[INFO] LOCAL_CLIENT_LOG_DIR_PATH_DOCKER=$LOCAL_CLIENT_LOG_DIR_PATH_DOCKER"

export CLIENT_LOG_FILE_PATH_DOCKER=$LOCAL_CLIENT_LOG_DIR_PATH_DOCKER/snowflake_ssm_rt.log
export CLIENT_LOG_FILE_PATH=$LOCAL_CLIENT_LOG_DIR_PATH/snowflake_ssm_rt.log
echo "[INFO] CLIENT_LOG_FILE_PATH=$CLIENT_LOG_FILE_PATH"
echo "[INFO] CLIENT_LOG_FILE_PATH_DOCKER=$CLIENT_LOG_FILE_PATH_DOCKER"

export CLIENT_KNOWN_SSM_FILE_PATH_DOCKER=$LOCAL_CLIENT_LOG_DIR_PATH_DOCKER/rt_jenkins_log_known_ssm.txt
export CLIENT_KNOWN_SSM_FILE_PATH=$LOCAL_CLIENT_LOG_DIR_PATH/rt_jenkins_log_known_ssm.txt
echo "[INFO] CLIENT_KNOWN_SSM_FILE_PATH=$CLIENT_KNOWN_SSM_FILE_PATH"
echo "[INFO] CLIENT_KNOWN_SSM_FILE_PATH_DOCKER=$CLIENT_KNOWN_SSM_FILE_PATH_DOCKER"

# To close log analyze, just set ENABLE_CLIENT_LOG_ANALYZE to not "true", e.g. "false".
if [[ "$is_old_driver" != "true" ]]; then
    export ENABLE_CLIENT_LOG_ANALYZE=true
else
    # Disable the secret log analyzer for the old client tests, because the old drivers don't necessarily have the fixes.
    # It is good enough to check for the latest JDBC driver
    export ENABLE_CLIENT_LOG_ANALYZE=false
fi

# The new complex password we use for jenkins test
export SNOWFLAKE_TEST_PASSWORD_NEW="ThisIsRandomPassword123!"

LOG_PROPERTY_FILE_DOCKER=$(cd "$(dirname "${BASH_SOURCE[0]}")/.."; pwd)/src/test/resources/logging.properties

export CLIENT_DRIVER_NAME=JDBC

function setup_log_env() {
    sed -i "s|^java.util.logging.FileHandler.pattern.*|java.util.logging.FileHandler.pattern = $CLIENT_LOG_FILE_PATH_DOCKER|" ${LOG_PROPERTY_FILE_DOCKER}

    if [[ ! -d ${LOCAL_CLIENT_LOG_DIR_PATH_DOCKER} ]]; then
      mkdir -p ${LOCAL_CLIENT_LOG_DIR_PATH_DOCKER}
    fi

    if [[ -f $CLIENT_KNOWN_SSM_FILE_PATH_DOCKER ]]; then
        rm -f $CLIENT_KNOWN_SSM_FILE_PATH_DOCKER
    fi
    touch $CLIENT_KNOWN_SSM_FILE_PATH_DOCKER
}
