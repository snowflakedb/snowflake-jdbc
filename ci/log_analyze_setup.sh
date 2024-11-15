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

LOG_PROPERTY_FILE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.."; pwd)/src/test/resources/logging.properties

export CLIENT_DRIVER_NAME=JDBC

function setup_log_env() {
    if [[ "$WORKSPACE" == "/mnt/workspace" ]]; then
        CLIENT_LOG_DIR_PATH=$LOCAL_CLIENT_LOG_DIR_PATH_DOCKER
        CLIENT_LOG_FILE_PATH=$CLIENT_LOG_FILE_PATH_DOCKER
        CLIENT_KNOWN_SSM_FILE_PATH=$CLIENT_KNOWN_SSM_FILE_PATH_DOCKER
    else
        CLIENT_LOG_DIR_PATH=$LOCAL_CLIENT_LOG_DIR_PATH
        CLIENT_LOG_FILE_PATH=$CLIENT_LOG_FILE_PATH
        CLIENT_KNOWN_SSM_FILE_PATH=$CLIENT_KNOWN_SSM_FILE_PATH
    fi
    echo "[INFO] CLIENT_LOG_DIR_PATH=$CLIENT_LOG_DIR_PATH"  
    echo "[INFO] CLIENT_LOG_FILE_PATH=$CLIENT_LOG_FILE_PATH"
    echo "[INFO] CLIENT_KNOWN_SSM_FILE_PATH=$CLIENT_KNOWN_SSM_FILE_PATH"
    echo "[INFO] Replace file handler for log file $LOG_PROPERTY_FILE"

    sed  -i'' -e "s|^java.util.logging.FileHandler.pattern.*|java.util.logging.FileHandler.pattern = $CLIENT_LOG_FILE_PATH|" ${LOG_PROPERTY_FILE}

    if [[ ! -d ${CLIENT_LOG_DIR_PATH} ]]; then
      echo "[INFO] create client log directory $CLIENT_LOG_DIR_PATH"
      mkdir -p ${CLIENT_LOG_DIR_PATH}
    fi

    if [[ -f $CLIENT_KNOWN_SSM_FILE_PATH ]]; then
        rm -f $CLIENT_KNOWN_SSM_FILE_PATH
    fi

    touch $CLIENT_KNOWN_SSM_FILE_PATH
    echo "[INFO] finish setup log env"
}
