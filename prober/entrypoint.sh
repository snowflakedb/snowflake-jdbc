#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Assign arguments to variables for clarity
JAVA_VERSION_ID=""
JDBC_VERSION=""

if [[ "$1" == "--java_version" && "$3" == "--driver_version" ]]; then
    JAVA_VERSION_ID="$2"
    JDBC_VERSION="$4"
else
  echo "Error: Missing required arguments."
  echo "Usage: $0 --java_version <java_version_id> --driver_version <jdbc_version> [prober_arguments...]"
  echo "Example: $0 --java_version 17.0.10-tem --driver_version 3.24.2 --host localhost --port 8080 --user admin --password secret"
  exit 1
fi

# Define key directories
SDKMAN_DIR="/root/.sdkman"
DRIVERS_DIR="/opt/jdbc_drivers"
PROBER_APP_DIR="/app"

# Get all arguments starting from the third one to pass to the Java application
PROBER_ARGS=("${@}")

JDBC_DRIVER_JAR_PATH="${DRIVERS_DIR}/snowflake-jdbc-${JDBC_VERSION}.jar"
CURRENT_CLASSPATH="${PROBER_APP_DIR}:${JDBC_DRIVER_JAR_PATH}"

# Construct the command to run the pre-compiled Prober application
RUN_PROBER_COMMAND="source \"${SDKMAN_DIR}/bin/sdkman-init.sh\" >/dev/null 2>&1 && \
                    sdk use java \"${JAVA_VERSION_ID}\" >/dev/null 2>&1 && \
                    java -cp \"${CURRENT_CLASSPATH}\" --add-opens=java.base/java.nio=ALL-UNNAMED com.snowflake.client.jdbc.prober.Prober \$@"

# Execute the Prober
bash -c "${RUN_PROBER_COMMAND}" _ "${PROBER_ARGS[@]}"

exit 0