timestamps {
  node('regular-memory-slave') {
    stage('checkout') {
      scmInfo = checkout scm
      println("${scmInfo}")
      env.GIT_BRANCH = scmInfo.GIT_BRANCH

    }

    stage('build') {
      sh '''\
        |export JAVA_HOME=/usr/java/latest
        |export PATH=$JAVA_HOME/bin:$PATH
        |export GIT_BRANCH=${GIT_BRANCH}
        |$WORKSPACE/ci/build.sh
        |echo "Branch is : ${GIT_BRANCH}"
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


      stage('Test') {
        parallel (
          'JDBC3': { build job: 'RT-LanguageJDBC3-PC',parameters: params
              },
          'JDBC2': { build job: 'RT-LanguageJDBC2-PC',parameters: params
              }
        )
      }
  }
}
