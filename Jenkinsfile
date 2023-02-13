import groovy.json.JsonOutput

pipeline {
  // TODO Please migrate this to C7 as sfc-dev2 servers do not support c6 nodes
  agent { label 'regular-memory-node' }
  options { timestamps() }
  environment {
    COMMIT_SHA_LONG = sh(returnStdout: true, script: "echo \$(git rev-parse " + "HEAD)").trim()

    // environment variables for semgrep_agent (for findings / analytics page)
    // remove .git at the end
    // remove SCM URL + .git at the end

    BASELINE_BRANCH = "${env.CHANGE_TARGET}"
  }
  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }
  }
}

timestamps {
  // TODO Please migrate this to C7 as sfc-dev2 servers do not support c6 nodes
  node('regular-memory-node') {
    stage('checkout') {
      scmInfo = checkout scm
      println("${scmInfo}")
      env.GIT_BRANCH = scmInfo.GIT_BRANCH
    }

    stage('Build') {
      sh '''\
        |export JAVA_HOME=/usr/java/latest
        |export PATH=$JAVA_HOME/bin:$PATH
        |export GIT_BRANCH=${GIT_BRANCH}
        |$WORKSPACE/ci/build.sh
      '''.stripMargin()
    }

    params = [
      string(name: 'client_git_branch', value: scmInfo.GIT_BRANCH),
      string(name: 'client_git_commit', value: scmInfo.GIT_COMMIT),
      string(name: 'branch', value: 'main'),
      string(name: 'TARGET_DOCKER_TEST_IMAGE', value: 'jdbc-centos6-default'),
      string(name: 'parent_job', value: env.JOB_NAME),
      string(name: 'parent_build_number', value: env.BUILD_NUMBER),
      string(name: 'timeout_value', value: '420'),
      string(name: 'PR_Key', value: scmInfo.GIT_BRANCH.substring(3))
    ]
    stage('Test') {
      try {
        def commit_hash = "main" // default which we want to override
        def bptp_tag = "bptp-built"
        def response = authenticatedGithubCall("https://api.github.com/repos/snowflakedb/snowflake/git/ref/tags/${bptp_tag}")
        commit_hash = response.object.sha
        // Append the bptp-built commit sha to params
        params += [string(name: 'svn_revision', value: commit_hash)]
      } catch(Exception e) {
        println("Exception computing commit hash from: ${response}")
      }
      parallel (
        'Test JDBC 1': { build job: 'RT-LanguageJDBC1-PC',parameters: params
            },
        'Test JDBC 2': { build job: 'RT-LanguageJDBC2-PC',parameters: params
            },
        'Test JDBC 3': { build job: 'RT-LanguageJDBC3-PC',parameters: params
            },
        'Test JDBC 4': { build job: 'RT-LanguageJDBC4-PC',parameters: params
            },
        'CodeCoverage JDBC': { build job: 'RT-LanguageJDBC-CodeCoverage-PC',parameters: params
            }
      )
    }
  }
}

// Generic github API function
// url : API url to call get command
def authenticatedGithubCall(url) {
  withCredentials([
        usernamePassword(credentialsId: 'jenkins-snowflakedb-github-app',
          usernameVariable: 'GITHUB_USER',
          passwordVariable: 'GITHUB_TOKEN'),
      ]) {
    try {
      def encodedAuth = Base64.getEncoder().encodeToString(
        "${GITHUB_USER}:${GITHUB_TOKEN}".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      )
      def authHeaderValue = "Basic ${encodedAuth}"
      def connection = new URL(url).openConnection()
      connection.setRequestProperty("Authorization", authHeaderValue)
      if (connection.getResponseCode() >= 300) {
        println("ERROR: Status fetch from ${url} returned ${connection.getResponseCode()}")
        println(connection.getErrorStream().getText())
        return null
      }
      return new groovy.json.JsonSlurperClassic().parseText(connection.getInputStream().getText())
    } catch(Exception e) {
      println("Exception fetching ${url}: ${e}")
      return null
    }
  }
}

def wgetUpdateGithub(String state, String folder, String targetUrl, String seconds) {
    def ghURL = "https://api.github.com/repos/snowflakedb/snowflake-jdbc/statuses/$COMMIT_SHA_LONG"
    def data = JsonOutput.toJson([state: "${state}", context: "jenkins/${folder}",target_url: "${targetUrl}"])
    sh "wget ${ghURL} --spider -q --header='Authorization: token $GIT_PASSWORD' --post-data='${data}'"
}
