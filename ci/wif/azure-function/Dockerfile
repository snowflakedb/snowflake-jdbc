# Build stage
FROM --platform=linux/amd64 mcr.microsoft.com/azure-functions/java:4-java17-build AS build

COPY azure-function /src/azure-function
COPY shared /src/shared

# Build the function
RUN cd /src/azure-function && \
    MAVEN_OPTS="-Xmx4g -Xms1g" mvn clean package -DskipTests && \
    mkdir -p /home/site/wwwroot && \
    cp -r target/azure-functions/drivers-wif-e2e/* /home/site/wwwroot/

# Runtime stage
FROM --platform=linux/amd64 mcr.microsoft.com/azure-functions/java:4-java17

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH" \
    AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \
    MAVEN_OPTS="-Xmx4g -Xms1g"

COPY --from=build /home/site/wwwroot /home/site/wwwroot
