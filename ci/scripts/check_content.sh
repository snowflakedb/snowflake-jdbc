#!/bin/bash -e

# scripts used to check if all dependency is shaded into snowflake internal path

package_modifier=$1

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if jar $DIR/../../target/snowflake-jdbc${package_modifier}.jar | awk '{print $8}' | grep -v -E "/$" | grep -v -E "^(net|com)/snowflake" | grep -v -E "(com|net)/\$" | grep -v -E "^META-INF" | grep -v -E "^iso3166_" | grep -v -E "^mozilla" | grep -v -E "^com/sun/jna" | grep -v com/sun/ | grep -v mime.types | grep -v -E "^com/github/luben/zstd/" | grep -v -E "^aix/" | grep -v -E "^darwin/" | grep -v -E "^freebsd/" | grep -v -E "^linux/" | grep -v -E "^win/"; then
  echo "[ERROR] JDBC jar includes class not under the snowflake namespace"
  exit 1
fi

if jar tvf $DIR/../../target/snowflake-jdbc${package_modifier}.jar | awk '{print $8}' | grep -E "^META-INF/versions/.*.class" | grep -v -E "^META-INF/versions/.*/(net|com)/snowflake"; then
  echo "[ERROR] JDBC jar includes multi-release classes not under the snowflake namespace"
  exit 1
fi
