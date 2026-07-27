#!/bin/bash -e

if [[ -z "$1" ]]; then
    echo First argument must be new version to set
    exit 1
fi

version=$1
version_without_snapshot=${version%-*}

# prepare release with maven (version.properties is populated by Maven resource filtering)
./mvnw -f parent-pom.xml versions:set -DnewVersion=$version -DgenerateBackupPoms=false

if [[ "$version" == *-SNAPSHOT ]]; then
sed -i '' '3a\
- v'"$version
" CHANGELOG.md
fi

# add changelog entry but only when releasing version without snapshot
if [[ "$version" == "$version_without_snapshot" ]]; then
  sed -i '' "4s/.*/- v$version/" CHANGELOG.md
fi
