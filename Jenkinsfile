timestamps {
  node('regular-memory-slave') {
    stage('checkout') {
      scmInfo = checkout scm
      println("${scmInfo}")
    }

    stage('build') {
      sh '''\
        |export JAVA_HOME=/usr/java/latest
        |export PATH=$JAVA_HOME/bin:$PATH
        |$WORKSPACE/ci/build.sh
      '''.stripMargin()
      sh '''\
        |cat <<PARAMS > $WORKSPACE/test_properties.txt
        |svn_revision=master
        |branch=master
        |client_git_commit=$GIT_COMMIT
        |client_git_branch=$GIT_BRANCH
        |parent_job=$JOB_NAME
        |parent_build_number=$BUILD_NUMBER
        |TARGET_DOCKER_TEST_IMAGE=jdbc-centos6-default
        |PARAMS
      '''.stripMargin()
    }
    params = [
      string(name: 'client_git_branch', value: scmInfo.GIT_BRANCH),
      string(name: 'client_git_commit', value: scmInfo.GIT_COMMIT),
      string(name: 'svn_revision', value: 'master'),
      string(name: 'branch', value: 'master'),
      string(name: 'TARGET_DOCKER_TEST_IMAGE', value: 'jdbc-centos6-default'),
      string(name: 'parent_job', value: env.JOB_NAME),
      string(name: 'parent_build_number', value: env.BUILD_NUMBER)
    ]
    build job: 'RT-LanguageJDBC1-PC',
      parameters: params,
      wait: false,
      propagate: false
    build job: 'RT-LanguageJDBC2-PC',
      parameters: params,
      wait: false,
      propagate: false
    build job: 'RT-LanguageJDBC3-PC',
      parameters: params,
      wait: false,
      propagate: false
    build job: 'RT-LanguageJDBC4-PC',
      parameters: params,
      wait: false,
      propagate: false
  }
}
