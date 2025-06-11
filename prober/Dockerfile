FROM ubuntu:24.04

# boilerplate labels required by validation when pushing to ACR, ECR & GCR
LABEL org.opencontainers.image.source="https://github.com/snowflakedb/snowflake-jdbc"
LABEL com.snowflake.owners.email="triage-snow-drivers-warsaw-dl@snowflake.com"
LABEL com.snowflake.owners.slack="triage-snow-drivers-warsaw-dl"
LABEL com.snowflake.owners.team="Snow Drivers"
LABEL com.snowflake.owners.jira_area="Developer Platform"
LABEL com.snowflake.owners.jira_component="JDBC Driver"
# fake layers label to pass the validation
LABEL com.snowflake.ugcbi.layers="sha256:850959b749c07b254308a4d1a84686fd7c09fcb94aeae33cc5748aa07e5cb232,sha256:b79d3c4628a989cbb8bc6f0bf0940ff33a68da2dca9c1ffbf8cfb2a27ac8d133,sha256:1cbcc0411a84fbce85e7ee2956c8c1e67b8e0edc81746a33d9da48c852037c3e,sha256:07e89b796f91d37255c6eec926b066d6818f3f2edc344a584d1b9566f77e1c27,sha256:84ff92691f909a05b224e1c56abb4864f01b4f8e3c854e4bb4c7baf1d3f6d652,sha256:3ab72684daee4eea64c3ae78a43ea332b86358446b6f2904dca4b634712e1537"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    zip \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV SDKMAN_DIR="/root/.sdkman"

RUN curl -s "https://get.sdkman.io?rcupdate=false&ci=false" | bash

# Ensure the Java versions are valid SDKMAN identifiers.
ARG JAVA_VERSIONS="11.0.22-tem 17.0.10-tem"
# Ensure these Snowflake JDBC versions exist on Maven Central.
ARG SNOWFLAKE_JDBC_VERSIONS="3.23.2 3.24.2"

# Install Java versions
RUN \
  SAVED_IFS=$IFS; \
  IFS=' '; \
  for java_version_full in $JAVA_VERSIONS; do \
    bash -c "source ${SDKMAN_DIR}/bin/sdkman-init.sh && \
        sdk install java ${java_version_full}"; \
  done; \
  IFS=$SAVED_IFS;

# Download specified Snowflake JDBC driver versions from Maven Central
ENV DRIVERS_DIR="/opt/jdbc_drivers"
RUN mkdir -p ${DRIVERS_DIR}

RUN \
  SAVED_IFS=$IFS; \
  IFS=' '; \
  for jdbc_version in $SNOWFLAKE_JDBC_VERSIONS; do \
    DRIVER_URL="https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${jdbc_version}/snowflake-jdbc-${jdbc_version}.jar"; \
    curl -fSL -o "${DRIVERS_DIR}/snowflake-jdbc-${jdbc_version}.jar.tmp" "${DRIVER_URL}" && \
    mv "${DRIVERS_DIR}/snowflake-jdbc-${jdbc_version}.jar.tmp" "${DRIVERS_DIR}/snowflake-jdbc-${jdbc_version}.jar"; \
  done; \
  IFS=$SAVED_IFS;

WORKDIR /app

# Create the package directory structure for the Prober
RUN mkdir -p com/snowflake/client/jdbc/prober

# Copy the Prober.java source code into the correct package structure
COPY /src/main/java/com/snowflake/client/jdbc/prober/Prober.java com/snowflake/client/jdbc/prober/Prober.java

# Copy the entrypoint script into the image and make it executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint script to be executed when the container starts
ENTRYPOINT ["/entrypoint.sh"]
