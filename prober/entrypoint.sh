#!/bin/bash

# Define key directories
SDKMAN_DIR="/root/.sdkman"
DRIVERS_DIR="/opt/jdbc_drivers"
PROBER_APP_DIR="/app"

# --- 1. Discover Installed Java Versions ---
INSTALLED_JAVA_VERSIONS=$(bash -c "source ${SDKMAN_DIR}/bin/sdkman-init.sh >/dev/null 2>&1 && \
    sdk list java | grep ' local only ' | awk '{print \$NF}' | sed 's/[\*>]//g' | sort -u")

# --- 2. Discover Available JDBC Driver Versions ---
AVAILABLE_JDBC_VERSIONS=$(ls "${DRIVERS_DIR}" 2>/dev/null | grep '^snowflake-jdbc-.*\.jar$' | sed -E 's/snowflake-jdbc-(.*)\.jar/\1/' | sort -u)

# --- 3. Iterate and Run Prober for each combination ---
# Convert multi-line strings of versions into arrays for easier iteration
readarray -t JAVA_VERSION_ARRAY <<<"$INSTALLED_JAVA_VERSIONS"
readarray -t JDBC_VERSION_ARRAY <<<"$AVAILABLE_JDBC_VERSIONS"

for java_version_id in "${JAVA_VERSION_ARRAY[@]}"; do
  for jdbc_version in "${JDBC_VERSION_ARRAY[@]}"; do
    JDBC_DRIVER_JAR_NAME="snowflake-jdbc-${jdbc_version}.jar"
    JDBC_DRIVER_JAR_PATH="${DRIVERS_DIR}/${JDBC_DRIVER_JAR_NAME}"
    CURRENT_CLASSPATH="${PROBER_APP_DIR}:${JDBC_DRIVER_JAR_PATH}"

    COMPILE_COMMAND="source ${SDKMAN_DIR}/bin/sdkman-init.sh >/dev/null 2>&1 && \
                     sdk use java ${java_version_id} >/dev/null 2>&1 && \
                     cd \"${PROBER_APP_DIR}\" && \
                     javac -cp \"${CURRENT_CLASSPATH}\" com/snowflake/client/jdbc/prober/Prober.java"
    bash -c "${COMPILE_COMMAND}"

    # Run the Prober Application
    RUN_PROBER_COMMAND="source ${SDKMAN_DIR}/bin/sdkman-init.sh >/dev/null 2>&1 && \
                        sdk use java ${java_version_id} >/dev/null 2>&1 && \
                        java -cp \"${CURRENT_CLASSPATH}\" --add-opens=java.base/java.nio=ALL-UNNAMED com.snowflake.client.jdbc.prober.Prober \$@"
    bash -c "${RUN_PROBER_COMMAND}" _ "$@"

  done # End of JDBC driver loop
done # End of Java version loop

exit 0
