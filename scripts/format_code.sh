#!/bin/bash -e
#
# Format JDBC source code with the codestyle.
#
# Note: mainly internal use.
#
set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/_init.sh

PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')

if [[ "$PLATFORM" == "darwin" ]]; then
    echo "[INFO] no MacOSX support."
    exit 0
fi

SNOWFLAKE_DEP_BASE_DIR=$HOME/SnowflakeDep

INTELLIJ_VERSION=2018.3.5
INTELLIJ_DIR_NAME=idea-IC-183.5912.21
TAR_FILE_NAME=ideaIC-${INTELLIJ_VERSION}.tar.gz
TAR_FILE_SIZE=543015318
HOST_TYPE=linux
TIMEOUT=timeout
STAT_CMD=("stat" "-c%s")
FOLDER_HASH="4f57bad6ae01032ac21dc04c25dcec8a"

SNOWFLAKE_DEP_DIR=$SNOWFLAKE_DEP_BASE_DIR/$HOST_TYPE
DIR_TARGET_NAME=$SNOWFLAKE_DEP_DIR/${INTELLIJ_DIR_NAME}


trap 'download_tarball_finish "$SNOWFLAKE_DEP_DIR" "$DIR_TARGET_NAME" "$TAR_FILE_NAME"' EXIT
pushd .
    download_tarball "$SNOWFLAKE_DEP_DIR" "$DIR_TARGET_NAME" "$TAR_FILE_NAME" "$TAR_FILE_SIZE" "$FOLDER_HASH"
popd

#if ps -ewo args | grep -v 'grep' | grep -q 'com.intellij.idea.Main'; then
#    echo "[ERROR] Exit IntelliJ and run the script again"
#    exit 1
#fi

SOURCE_DIR=$THIS_DIR/../src

old_hash=$(find $SOURCE_DIR -name "*.java" -exec md5sum {} \; | cut -d " " -f1 | sort -k 1 | md5sum | cut -d " " -f1)
echo "Code hash before format: $old_hash"
$DIR_TARGET_NAME/bin/format.sh \
    -mask "*.java" \
    -settings $THIS_DIR/../intellij-codestyle.xml \
    -R $SOURCE_DIR

new_hash=$(find $SOURCE_DIR -name "*.java" -exec md5sum {} \; | cut -d " " -f1 | sort -k 1 | md5sum | cut -d " " -f1)
echo "Code hash after  format: $new_hash"
if [[ "$old_hash" != "$new_hash" ]]; then
    if [[ -n "$SF_ENFORCE_CLIENT_FORMAT" ]] && [[ $SF_ENFORCE_CLIENT_FORMAT -eq 1 ]]; then
      (>&2 echo "[ERROR] Must run $THIS_DIR/format_code.sh before commiting, pre-commiting or running tests")
      exit 1 
    fi
fi
exit 0
