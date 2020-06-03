#!/bin/bash -e
#
# Set GIT info
#
if [[ -z "$GITHUB_ACTIONS" ]]; then
    #
    # set Jenkins GIT parameters propagated from Build job.
    # 
    export client_git_url=${client_git_url:-https://github.com/snowflakedb/snowflake-jdbc.git}
    export client_git_branch=${client_git_branch:-origin/$(git rev-parse --abbrev-ref HEAD)}
    export client_git_commit=${client_git_commit:-$(git log --pretty=oneline | head -1 | awk '{print $1}')}
else
    if [[ "$CLOUD_PROVIDER" == "AZURE" ]]; then
        gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../parameters.json $THIS_DIR/../.github/workflows/parameters_azure.json.gpg
    elif [[ "$CLOUD_PROVIDER" == "GCP" ]]; then
        gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../parameters.json $THIS_DIR/../.github/workflows/parameters_gcp.json.gpg
    else
        gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../parameters.json $THIS_DIR/../.github/workflows/parameters_aws.json.gpg
    fi
    export client_git_url=https://github.com/${GITHUB_REPOSITORY}.git
    export client_git_branch=origin/$(basename ${GITHUB_REF})
    export client_git_commit=${GITHUB_SHA}
fi

#
# set GIT parameters used in the following scripts
#
export GIT_URL=$client_git_url
export GIT_BRANCH=$client_git_branch
export GIT_COMMIT=$client_git_commit
echo "GIT_URL: $GIT_URL, GIT_BRANCH: $GIT_BRANCH, GIT_COMMIT: $GIT_COMMIT"
