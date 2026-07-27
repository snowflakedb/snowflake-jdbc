#!/bin/bash -e

set -o pipefail

export SF_ENABLE_EXPERIMENTAL_AUTHENTICATION=true

./mvnw -Dmaven.repo.local=/tmp/maven-repo \
  -DjenkinsIT \
  -Dnet.snowflake.jdbc.temporaryCredentialCacheDir=/tmp/workspace \
  -Dnet.snowflake.jdbc.ocspResponseCacheDir=/tmp/workspace \
  -Djava.io.tmpdir=/tmp/workspace \
  -Djacoco.skip.instrument=true \
  -Dskip.unitTests=true \
  -DintegrationTestSuites=WIFTestSuite \
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
  -Dnot-self-contained-jar \
  -Denforcer.skip=true \
  -Dmaven.javadoc.skip=true \
  verify \
  --batch-mode --show-version
