#!/bin/bash -e
#
# Run Travis Build and Tests
#
set -o pipfail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

function travis_fold_start() {
    local name=$1
    local message=$2
    echo "travis_fold:start:$name"
    tput setaf 3 || true
    echo $message
    tput sgr0 || true
    export travis_fold_name=$name
}

function travis_fold_end() {
    echo "travis_fold:end:$travis_fold_name"
    unset travis_fold_name
}

function finish {
    travis_fold_start drop_schema "Drop test schema"
    python $DIR/drop_schema.py 
    travis_fold_end
}

# build
travis_fold_start build "Build JDBC driver"
mvn install -DskipTests=true -Dmaven.javadoc.skip=true -B -V
travis_fold_end

trap finish EXIT
