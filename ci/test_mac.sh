#!/bin/bash -e
#
# Test JDBC for Mac
#

echo "DOWNLOADED"
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/_init.sh
source $THIS_DIR/scripts/set_git_info.sh


export WORKSPACE=$GITHUB_WORKSPACE
export SOURCE_ROOT=$GITHUB_WORKSPACE

python3 --version
python3 -m venv venv
source venv/bin/activate
pip3 install -U pip
pip3 install -U snowflake-connector-python
$THIS_DIR/container/test_component.sh
