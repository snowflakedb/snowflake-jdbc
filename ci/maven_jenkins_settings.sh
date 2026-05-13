#!/usr/bin/env bash
#
# Sourced helper that points Maven at the Snowflake-internal Artifactory mirror
# when running on Jenkins. No-op everywhere else (GitHub Actions, local dev).
#
# Sets MVN_SETTINGS_ARG so callers can write `mvn $MVN_SETTINGS_ARG ...`.
# When sourced inside a container that bind-mounts the repo, BASH_SOURCE
# resolves to the in-container path so -s still points at the right file.
if [[ -n "$JENKINS_HOME" ]]; then
  _MAVEN_JENKINS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  export MVN_SETTINGS_ARG="-s ${_MAVEN_JENKINS_DIR}/maven-settings.xml"
else
  export MVN_SETTINGS_ARG=""
fi
