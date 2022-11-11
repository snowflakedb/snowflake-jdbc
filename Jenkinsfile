import groovy.json.JsonOutput

pipeline {
  agent { label 'regular-memory-node' }
  options { timestamps() }
  environment {
    COMMIT_SHA_LONG = sh(returnStdout: true, script: "echo \$(git rev-parse " + "HEAD)").trim()
    SEMGREP_DEPLOYMENT_ID = 1
    INPUT_PUBLISHURL      = "https://semgrep.snowflake.com"

    // environment variables for semgrep_agent (for findings / analytics page)
    // remove .git at the end
    SEMGREP_REPO_URL = env.GIT_URL.replaceFirst(/^(.*).git$/,'$1')
    SEMGREP_BRANCH = "${CHANGE_BRANCH}"
    SEMGREP_JOB_URL = "${BUILD_URL}"
    // remove SCM URL + .git at the end
    SEMGREP_REPO_NAME = env.GIT_URL.replaceFirst(/^https:\/\/github.com\/(.*).git$/, '$1')

    SEMGREP_COMMIT = "${GIT_COMMIT}"
    SEMGREP_PR_ID = "${env.CHANGE_ID}"
    BASELINE_BRANCH = "${env.CHANGE_TARGET}"
  }
  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }
    stage('Semgrep_agent') {
      agent {
        docker {
          label 'parallelizable-c7'
          image 'nexus.int.snowflakecomputing.com:8087/returntocorp/semgrep-agent:v1'
          args '-u root'
        }
      }
      when {
        expression { env.CHANGE_ID && env.BRANCH_NAME.startsWith("PR-") }
      }
      steps{
        wrap([$class: 'MaskPasswordsBuildWrapper']) {
          withCredentials([
            [$class: 'UsernamePasswordMultiBinding', credentialsId:
                  'b4f59663-ae0a-4384-9fdc-c7f2fe1c4fca', usernameVariable:
                  'GIT_USERNAME', passwordVariable: 'GIT_PASSWORD'],
            string(credentialsId:'SEMGREP_APP_TOKEN', variable: 'SEMGREP_APP_TOKEN'),

          ]) {
            script {
              try {
                sh 'export SEMGREP_DIR=semgrep-scan-$(pwd | rev | cut -d \'/\' -f1 | rev) && mkdir -p ../$SEMGREP_DIR && cp -R . ../$SEMGREP_DIR  && cd ../$SEMGREP_DIR && git fetch https://$GIT_USERNAME:$GIT_PASSWORD@github.com/$SEMGREP_REPO_NAME.git $BASELINE_BRANCH:refs/remotes/origin/$BASELINE_BRANCH && python -m semgrep_agent --baseline-ref $(git merge-base origin/$BASELINE_BRANCH HEAD) --publish-token $SEMGREP_APP_TOKEN --publish-deployment $SEMGREP_DEPLOYMENT_ID && cd ../ && rm -r $SEMGREP_DIR'
                wgetUpdateGithub('success', 'semgrep', "${BUILD_URL}", '123')
              } catch (err) {
                wgetUpdateGithub('failure', 'semgrep', "${BUILD_URL}", '123')
              }
            }
          }
        }
      }
    }
  }
}

timestamps {
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
      string(name: 'svn_revision', value: 'main'),
      string(name: 'branch', value: 'main'),
      string(name: 'TARGET_DOCKER_TEST_IMAGE', value: 'jdbc-centos6-default'),
      string(name: 'parent_job', value: env.JOB_NAME),
      string(name: 'parent_build_number', value: env.BUILD_NUMBER),
      string(name: 'timeout_value', value: '420'),
      string(name: 'PR_Key', value: scmInfo.GIT_BRANCH.substring(3))
    ]
    stage('Test') {
      commit_hash = "main" // default which we want to override
      bptp_tag = "bptp-built"
      // withCredentials([
      //       [$class: 'UsernamePasswordMultiBinding', credentialsId:
      //             'b4f59663-ae0a-4384-9fdc-c7f2fe1c4fca', usernameVariable:
      //             'GIT_USERNAME', passwordVariable: 'GIT_PASSWORD']
      //     ]) {
      //     commit_hash = getBPTPTagCommit(bptp_tag)
      // }
      def curStatus = authenticatedGithubCall("https://api.github.com/repos/snowflakedb/snowflake/git/ref/tags/${bptp_tag}")
      println(commit_hash)
      // TESTING
      // parallel (
      //   'Test JDBC 1': { build job: 'RT-LanguageJDBC1-PC',parameters: params
      //       },
      //   'Test JDBC 2': { build job: 'RT-LanguageJDBC2-PC',parameters: params
      //       },
      //   'Test JDBC 3': { build job: 'RT-LanguageJDBC3-PC',parameters: params
      //       },
      //   'Test JDBC 4': { build job: 'RT-LanguageJDBC4-PC',parameters: params
      //       },
      //   'CodeCoverage JDBC': { build job: 'RT-LanguageJDBC-CodeCoverage-PC',parameters: params
      //       }
      // )
    }
  }
}


// def getBPTPTagCommit(String tag) {
//   def url = "https://api.github.com/repos/snowflakedb/snowflake/git/ref/tags/${tag}"
//   return sh "curl -s -H 'Accept: application/vnd.github+json' -H 'Authorization: Bearer $GIT_PASSWORD' ${url} | jq -r .object.sha"
// }

def authenticatedGithubCall(url, postData=null) {
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
      if (postData) {
        connection.setRequestMethod("POST")
        connection.setDoOutput(true)
        connection.setRequestProperty("Content-Type", "application/json")
        connection.getOutputStream().write(postData.getBytes("UTF-8"));
      }
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
