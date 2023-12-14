#!/bin/bash -e

# scripts used to check if all dependency is shaded into snowflake internal path

package_modifier=$1

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if jar tvf $DIR/../../target/snowflake-jdbc${package_modifier}.jar  | awk '{print $8}' | grep -v -E "^(net|com)/snowflake" | grep -v -E "(com|net)/\$" | grep -v -E "^META-INF" | grep -v -E "^mozilla" | grep -v -E "^com/sun/jna" | grep -v com/sun/ | grep -v mime.types; then
  echo "[ERROR] JDBC jar includes class not under the snowflake namespace"
  exit 1
fi
