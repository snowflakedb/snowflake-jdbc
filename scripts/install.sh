#!/bin/bash -e
set -o pipefail

if [[ -n "$SNOWFLAKE_AZURE" ]]; then
    openssl aes-256-cbc -k "$super_azure_secret_password" -in parameters_az.json.enc -out parameters.json -d
else
 	openssl aes-256-cbc -k "$super_secret_password" -in parameters.json.enc -out parameters.json -d
fi

eval "${MATRIX_EVAL}"
sudo apt-get update
pyenv local 3.6
curl -O https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip --version
pip install -U virtualenv
virtualenv env
source env/bin/activate