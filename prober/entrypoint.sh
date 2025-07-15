#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

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

PROBER_ARGS=("${@}")

# Define key directories
SDKMAN_DIR="/app/.sdkman"
DRIVERS_DIR="/opt/jdbc_drivers"
PROBER_APP_DIR="/app"

source "${SDKMAN_DIR}/bin/sdkman-init.sh" >/dev/null 2>&1
sdk use java "${JAVA_VERSION_ID}" >/dev/null 2>&1

JDBC_DRIVER_JAR_PATH="${DRIVERS_DIR}/snowflake-jdbc-${JDBC_VERSION}.jar"
CURRENT_CLASSPATH="${PROBER_APP_DIR}:${JDBC_DRIVER_JAR_PATH}"

exec java -cp "${CURRENT_CLASSPATH}" \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          com.snowflake.client.jdbc.prober.Prober \
          "${PROBER_ARGS[@]}"

# This line will not be reached because 'exec' replaces the process.
# It would only be reached if 'exec java' itself failed to start the Java process.
echo "Error: Java application failed to start."
exit 1 # Exit with an error if Java failed to start
