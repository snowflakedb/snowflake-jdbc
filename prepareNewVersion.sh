#!/bin/bash -e

if [[ -z "$1" ]]; then
    echo First argument must be new version to set
    exit 1
fi

version=$1

# prepare release with maven
./mvnw -f parent-pom.xml versions:set -DnewVersion=$version -DgenerateBackupPoms=false

# update version in Driver code
version_without_snapshot=${version%-*}
file_with_version=src/main/java/net/snowflake/client/jdbc/SnowflakeDriver.java
tmp_file_with_version=${file_with_version}.tmp
sed -E "s/( implementVersion = )(.+)(;)/\1\"${version_without_snapshot}\"\3/" src/main/java/net/snowflake/client/jdbc/SnowflakeDriver.java > $tmp_file_with_version
mv $tmp_file_with_version $file_with_version

# add changelog entry but only when releasing version without snapshot
if [[ "$version" == "$version_without_snapshot" ]]; then
  changelog_file=CHANGELOG.rst
  tmp_changelog_file=${changelog_file}.bck
  echo "**JDBC Driver ${version}**" > $tmp_changelog_file
  echo "" >> $tmp_changelog_file
  echo "- \||Please Refer to Release Notes at https://docs.snowflake.com/en/release-notes/clients-drivers/jdbc" >> $tmp_changelog_file
  echo "" >> $tmp_changelog_file
  cat $changelog_file >> $tmp_changelog_file
  mv $tmp_changelog_file $changelog_file
fi
