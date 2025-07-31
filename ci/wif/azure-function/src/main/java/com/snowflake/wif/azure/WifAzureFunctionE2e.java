package com.snowflake.wif.azure;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class WifAzureFunctionE2e {

    @FunctionName("WifAzureFunctionE2e")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        try {
            context.getLogger().info("=== WIF Azure Function E2E started ===");
            validateQueryParameters(request);
            setSystemProperties(request);
            installJdkIfNeeded(context);

            String branch = request.getQueryParameters().get("BRANCH");
            String tarballUrl = buildTarballUrl(branch);

            downloadAndExtractRepository(tarballUrl, context);
            String repoFolderPath = findRepositoryFolder(context);
            int mavenExitCode = executeMavenBuild(repoFolderPath, context);

            return createResponse(request, mavenExitCode);
        } catch (Exception e) {
            context.getLogger().severe("Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage()).build();
        }
    }

    private void installJdkIfNeeded(ExecutionContext context) throws IOException, InterruptedException {
        try {
            ProcessBuilder pb = new ProcessBuilder("which", "javac");
            Process process = pb.start();
            if (process.waitFor() == 0) {
                context.getLogger().info("JDK already available");
                return;
            }
        } catch (Exception e) {
            context.getLogger().info("JDK not found, attempting to install");
        }

        String installCommand = "apt-get update && apt-get install -y openjdk-17-jdk";

        ProcessBuilder pb = new ProcessBuilder("bash", "-c", installCommand);
        Process process = pb.start();

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Failed to install JDK. Exit code: " + exitCode);
        }

        context.getLogger().info("JDK installed successfully");
    }
    
    private void validateQueryParameters(HttpRequestMessage<Optional<String>> request) {
        String wifHost = request.getQueryParameters().get("SNOWFLAKE_TEST_WIF_HOST");
        String wifAccount = request.getQueryParameters().get("SNOWFLAKE_TEST_WIF_ACCOUNT");
        String wifProvider = request.getQueryParameters().get("SNOWFLAKE_TEST_WIF_PROVIDER");
        String branch = request.getQueryParameters().get("BRANCH");

        if (wifHost == null) {
            throw new IllegalArgumentException("Missing required query parameter: SNOWFLAKE_TEST_WIF_HOST");
        }
        if (wifAccount == null) {
            throw new IllegalArgumentException("Missing required query parameter: SNOWFLAKE_TEST_WIF_ACCOUNT");
        }
        if (wifProvider == null) {
            throw new IllegalArgumentException("Missing required query parameter: SNOWFLAKE_TEST_WIF_PROVIDER");
        }
        if (branch == null) {
            throw new IllegalArgumentException("Missing required query parameter: BRANCH");
        }
    }
    
    private void setSystemProperties(HttpRequestMessage<Optional<String>> request) {
        String wifHost = request.getQueryParameters().get("SNOWFLAKE_TEST_WIF_HOST");
        String wifAccount = request.getQueryParameters().get("SNOWFLAKE_TEST_WIF_ACCOUNT");
        String wifProvider = request.getQueryParameters().get("SNOWFLAKE_TEST_WIF_PROVIDER");
        String branch = request.getQueryParameters().get("BRANCH");
        
        System.setProperty("SNOWFLAKE_TEST_WIF_HOST", wifHost);
        System.setProperty("SNOWFLAKE_TEST_WIF_ACCOUNT", wifAccount);
        System.setProperty("SNOWFLAKE_TEST_WIF_PROVIDER", wifProvider);
        System.setProperty("BRANCH", branch);
    }
    
    private String buildTarballUrl(String branch) {
        if (Pattern.matches("^PR-\\d+$", branch)) {
            String prNumber = branch.substring(3);
            return "https://github.com/snowflakedb/snowflake-jdbc/archive/refs/pull/" + prNumber + "/head.tar.gz";
        } else {
            return "https://github.com/snowflakedb/snowflake-jdbc/archive/refs/heads/" + branch + ".tar.gz";
        }
    }

    private void downloadAndExtractRepository(String tarballUrl, ExecutionContext context) throws IOException, InterruptedException {
        String tempDir = System.getProperty("java.io.tmpdir");
        String workDir = tempDir + "/wif-function-" + System.currentTimeMillis();
        File workingDirectory = new File(workDir);

        if (!workingDirectory.exists()) {
            workingDirectory.mkdirs();
        }

        String downloadCommand = "wget -O- " + tarballUrl + " | tar -xz";

        ProcessBuilder pb = new ProcessBuilder("bash", "-c", downloadCommand);
        pb.directory(workingDirectory);

        context.getLogger().info("Using wget to download: " + tarballUrl);

        Process process = pb.start();

        boolean finished = false;
        long startTime = System.currentTimeMillis();
        long downloadTimeoutMs = 300000;

        while (!finished) {
            try {
                int exitCode = process.exitValue();
                finished = true;
                if (exitCode != 0) {
                    throw new RuntimeException("Failed to download and extract tarball: " + tarballUrl + " (exit code: " + exitCode + ")");
                }
            } catch (IllegalThreadStateException e) {
                long currentTime = System.currentTimeMillis();

                if (currentTime - startTime > downloadTimeoutMs) {
                    process.destroyForcibly();
                    throw new RuntimeException("Download timeout after 5 minutes");
                }

                Thread.sleep(1000);
            }
        }
    }
    
    private String findRepositoryFolder(ExecutionContext context) {
        String tempDir = System.getProperty("java.io.tmpdir");
        File[] tempDirs = new File(tempDir).listFiles();
        
        if (tempDirs != null) {
            for (File dir : tempDirs) {
                if (dir.isDirectory() && dir.getName().startsWith("wif-function-")) {
                    File[] files = dir.listFiles();
                    if (files != null) {
                        for (File file : files) {
                            if (file.isDirectory() && file.getName().startsWith("snowflake-jdbc-")) {
                                return file.getAbsolutePath();
                            }
                        }
                    }
                }
            }
        }
        
        throw new RuntimeException("Driver repository folder not found");
    }
    
    private int executeMavenBuild(String repoFolderPath, ExecutionContext context) {
        Process process = null;
        try {
            String tmpDir = System.getProperty("java.io.tmpdir");
            File mavenRepo = new File(tmpDir, "maven-repo-" + System.currentTimeMillis());
            File workspace = new File(tmpDir, "workspace-" + System.currentTimeMillis());
            mavenRepo.mkdirs();
            workspace.mkdirs();

            ProcessBuilder pb = new ProcessBuilder(
                "bash", "-c",
                "cd " + repoFolderPath + " && " +
                "./mvnw -Dmaven.repo.local=" + mavenRepo.getAbsolutePath() + " " +
                "-DjenkinsIT " +
                "-Dnet.snowflake.jdbc.temporaryCredentialCacheDir=" + workspace.getAbsolutePath() + " " +
                "-Dnet.snowflake.jdbc.ocspResponseCacheDir=" + workspace.getAbsolutePath() + " " +
                "-Djava.io.tmpdir=" + workspace.getAbsolutePath() + " " +
                "-Djacoco.skip.instrument=true " +
                "-Dskip.unitTests=true " +
                "-DintegrationTestSuites=WIFTestSuite " +
                "-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn " +
                "-Dnot-self-contained-jar " +
                "-Dmaven.test.failure.ignore=false " +
                "-DMAVEN_OPTS=\"-Xmx1536m\" " +
                "-Dmaven.artifact.threads=1 " +
                "-Dmaven.compile.fork=false " +
                "verify --batch-mode --show-version --fail-fast --no-transfer-progress"
            );
            pb.redirectErrorStream(true);
            pb.environment().put("SNOWFLAKE_TEST_WIF_HOST", System.getProperty("SNOWFLAKE_TEST_WIF_HOST"));
            pb.environment().put("SNOWFLAKE_TEST_WIF_ACCOUNT", System.getProperty("SNOWFLAKE_TEST_WIF_ACCOUNT"));
            pb.environment().put("SNOWFLAKE_TEST_WIF_PROVIDER", System.getProperty("SNOWFLAKE_TEST_WIF_PROVIDER"));
            pb.environment().put("SF_ENABLE_EXPERIMENTAL_AUTHENTICATION", "true");

            process = pb.start();
            
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            
            Thread outputThread = new Thread(() -> {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        context.getLogger().info(line);
                    }
                } catch (IOException e) {
                    context.getLogger().warning("Error reading Maven output: " + e.getMessage());
                }
            });
            outputThread.start();
            
            if (!process.waitFor(25, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                return -2; // timeout
            }
            return process.exitValue();
            
        } catch (Exception e) {
            context.getLogger().severe("Maven build error: " + e.getMessage());
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
            return -1;
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
                try {
                    process.waitFor(10, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    private HttpResponseMessage createResponse(HttpRequestMessage<Optional<String>> request, int mavenExitCode) {
        if (mavenExitCode == 0) {
            return request.createResponseBuilder(HttpStatus.OK).build();
        } else {
            String mavenResult;
            String testResults;
            
            if (mavenExitCode == -2) {
                mavenResult = "TIMEOUT (exit code: " + mavenExitCode + ")";
                testResults = "Build timed out after 25 minutes";
            } else {
                mavenResult = "FAILED (exit code: " + mavenExitCode + ")";
                testResults = "Build or tests failed";
            }
            
            String response = String.format(
                    "MAVEN_RESULT=%s\nTEST_STATUS=%s",
                    mavenResult, testResults
            );
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(response).build();
        }
    }
}