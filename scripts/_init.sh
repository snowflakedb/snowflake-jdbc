#!/bin/bash -ex
#
# Util functions
#

#
# Downloads and extracts a tarball in a specified directory. Use download_tarball for the top level function call.
#
# Params:
#   dep_dir:       SnowflakeDep directory, e.g., $HOME/SnowflakeDep/linux
#   target_dir:    Target directory, e.g., $HOME/SnowflakeDep/linux/SimbaEngineSDK
#   tar_file_name: Tarball file name, e.g., SimbaEngineSDK_Release_Linux-x86_10.0.5.1021.tar.gz
#   tar_file_size: Tarball file size, e.g., 608867883, used to verify the content.
#   target_hash:   Hash value of all the contents under target_dir. Optional: no hash check if not provided
#
# Returns:
#   DIR_LOCK_CREATED: true if the lock file was created, or false if not. The caller must add download_tarball_finish()
#                     to trap EXIT.
#   RETMSG:           empty if no error or non-empty if errors.
#
download_tarball_main() {
    local dep_dir=$1
    local target_dir=$2
    local tar_file_name=$3
    local tar_file_size=$4
    local target_hash=$5

    local dir_lock_name=${target_dir}.lck
    local need_download=0
    local filesize=0

    local platform=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
    local host_type=linux
    local timeout_cmd=timeout
    local stat_cmd=("stat" "-c%s")
    local stat_ts_cmd=("stat" "-c%y")
    local tar_base_file_name=${tar_file_name%.tar.gz}

    DIR_LOCK_CREATED=false
    RETMSG=()
    if [[ "$platform" == "darwin" ]]; then
        timeout_cmd=gtimeout
        host_type=Mac
        stat_cmd=("stat" "-f%z")
        stat_ts_cmd=("stat" "-t" "%F %T")
    fi

    # check if the SDK is already installed
    mkdir -p "$dep_dir"
    echo 1>&2 "Created $dep_dir"

    # obtain a lock before checking integrity as the time for integrity check is not negligible
    if ! mkdir "$dir_lock_name"; then
        if test `find "$dir_lock_name" -mmin +60`; then
            # Lock older than 1 hour, removing the old lock: $dir_lock_name
            rm -rf "$dir_lock_name"
            # Creating a new lock: $dir_lock_name
            mkdir "$dir_lock_name"
        else
            # Lock is within 1 hour, aborting this setup"
            echo 1>&2 "Lock was created within last 1 hour. Waiting to be deleted by other process..."
            echo 1>&2 "$dir_lock_name"
            echo 1>&2 "Current: "$(date +'%F %T')
            echo 1>&2 "Created: "$(${stat_ts_cmd[@]} "$dir_lock_name")
            return 2
        fi
    fi
    DIR_LOCK_CREATED=true
    echo 1>&2 "Lock file is created $dir_lock_name"

    # It woud take about 8 seconds
    echo 1>&2 "Validating downloaded contents. May take up to 2 minutes."
    check_integrity "$target_dir" "$target_hash"
    already_exist=$?
    if [ $already_exist -eq 1 ]; then
        RETMSG+=("Good. $target_dir already exists.")
        return 0
    fi
    # download the tar ball

    cd "$dep_dir"

    if [[ ! -f $tar_file_name ]]; then
        need_download=1
    else
        filesize=$(${stat_cmd[@]} "$tar_file_name")
        if [[ ! $filesize -eq $tar_file_size ]]; then
            need_download=1
        fi
    fi

    if [[ $need_download -eq 1 ]]; then
        # Download the tarball, but don't wait longer than 3600 seconds 
        echo 1>&2 "Downloading s3://sfc-dev1-data/dependency/$tar_file_name..."
        $timeout_cmd -s 9 3600 aws --region us-west-2 s3 \
            cp --only-show-errors s3://sfc-dev1-data/dependency/$tar_file_name .

        if [[ ! -f $tar_file_name ]]; then
            echo 1>&2 "Failed to download $tar_file_name"
            return 1;
        fi

        filesize=$(${stat_cmd[@]} $tar_file_name)
        if [[ ! $filesize -eq $tar_file_size ]]; then
            echo 1>&2 "Downloaded tarball has incorrect size. got: $filesize, expected: $tar_file_size"
            echo 1>&2 "Make sure s3://sfc-dev1-data/dependency/$tar_file_name is a valid tarball."
            return 1;
        fi
    fi

    echo 1>&2 "Extracting $tar_file_name..."
    if ! tar xfz $tar_file_name; then
        echo 1>&2 "Failed to extract $tar_file_name in $dep_dir!"
        return 1
    fi
    RETMSG+=("Completed: $tar_file_name in $dep_dir.")
    return 0
}

#
# Clean up lock and tarball file
#
# Params:
#   DIR_LOCK_CREATED: true if the lock file was created, or false if not. The caller must add download_tarball_finish()
#                     to trap EXIT.
#
download_tarball_finish() {
    local dep_dir=$1
    local target_dir=$2
    local tar_file_name=$3

    if [[ "$DIR_LOCK_CREATED" == "true" ]]; then
        rm -rf "${target_dir}.lck" || echo "Could not remove lock dir: ${target_dir}.lck" >&2
    fi
    rm -f "$dep_dir/$tar_file_name" || echo "Failed to remove the tar file: $tar_file_name" >&2
}

#
# (MAIN) Downloads and extracts a tarball
#
download_tarball() {
    local dep_dir=$1
    local target_dir=$2
    local tar_file_name=$3
    local tar_file_size=$4
    local target_hash=$5

    while true; do
        if download_tarball_main "$dep_dir" "$target_dir" "$tar_file_name" "$tar_file_size" "$target_hash"; then
            for m in "${RETMSG[@]}"; do echo "$m"; done
            break
        fi
        exit_val=$?
        if [[ "$exit_val" -eq 1 ]]; then
            for m in "${RETMSG[@]}"; do echo "$m"; done
            exit 1
        fi
        for m in "${RETMSG[@]}"; do echo "$m"; done
        echo 1>&2 "Sleeping 10 seconds..."
        sleep 10
    done
}

#
# Initialize environments
#
init_environment() {
    local dir=$1

    # linux, darwin
    export PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
    # ensure the destination exists
    export DEPENDENCY_DIR=$dir/../Dependencies/$PLATFORM
    mkdir -p $DEPENDENCY_DIR
}

#
# check the integrity of target dir
# return 1 if the target dir exist and complete
# Note: only check the completeness when the target_hash is provided
#
check_integrity() {
    local target_dir=$1
    local target_hash=$2

    if [[ ! -d "$target_dir" ]]; then
        return 0;
    fi

    echo "Reference: $target_hash"
    # return true if the target hash is not provided
    if [ -z $target_hash ]; then
        return 1
    fi

    local actual_hash=$(find $target_dir -type f -exec md5sum '{}' + |  cut -d " " -f1 |sort -k 1 | md5sum | cut -d " " -f1)
    echo "Actual:    $actual_hash"
    if [ "$actual_hash" == "$target_hash" ]; then
        return 1
    fi
    return 0
}
