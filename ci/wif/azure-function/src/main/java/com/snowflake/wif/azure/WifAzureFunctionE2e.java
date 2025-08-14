package com.snowflake.wif.azure;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.snowflake.wif.common.WifTestHelper;

import java.io.File;
import java.util.Optional;

public class WifAzureFunctionE2e {

    private static class AzureLogger implements WifTestHelper.WifLogger {
        private final ExecutionContext context;
        
        public AzureLogger(ExecutionContext context) {
            this.context = context;
        }
        
        @Override
        public void log(String message) {
            context.getLogger().info(message);
        }
    }

    @FunctionName("WifAzureFunctionE2e")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        WifTestHelper.WifLogger logger = new AzureLogger(context);
        String workingDirectory = null;
        
        try {
            logger.log("=== WIF Azure Function E2E started ===");
            WifTestHelper.validateQueryParameters(request.getQueryParameters());
            installJdkIfNeeded(logger);

            String branch = request.getQueryParameters().get("BRANCH");
            String tarballUrl = WifTestHelper.buildTarballUrl(branch);

            // Download and extract to timestamp-based directory
            workingDirectory = WifTestHelper.downloadAndExtractRepository(tarballUrl, logger);
            String repoFolderPath = WifTestHelper.findRepositoryFolder(workingDirectory);
            WifTestHelper.makeExecutable(repoFolderPath, logger);
            int mavenExitCode = WifTestHelper.executeMavenBuild(repoFolderPath, System.getProperty("java.io.tmpdir"), 25, logger, request.getQueryParameters());

            return createResponse(request, mavenExitCode);
        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage()).build();
        } finally {
            WifTestHelper.cleanupWorkingDirectory(workingDirectory, logger);
        }
    }
    
    private HttpResponseMessage createResponse(HttpRequestMessage<Optional<String>> request, int mavenExitCode) {
        String responseBody = WifTestHelper.createMavenResultMessage(mavenExitCode, 25);
        
        if (mavenExitCode == 0) {
            return request.createResponseBuilder(HttpStatus.OK).body(responseBody).build();
        } else {
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(responseBody).build();
        }
    }

    private void installJdkIfNeeded(WifTestHelper.WifLogger logger) {
        try {
            logger.log("Checking Java environment in Azure Functions...");
            
            // Check if javac is already available
            try {
                ProcessBuilder javacCheck = new ProcessBuilder("javac", "-version");
                Process javacProcess = javacCheck.start();
                int javacExitCode = javacProcess.waitFor();
                
                if (javacExitCode == 0) {
                    logger.log("javac is already available");
                    return;
                }
            } catch (Exception e) {
                logger.log("javac not found, installing OpenJDK...");
            }
            
            logger.log("Installing OpenJDK 17 JDK...");
            
            try {
                ProcessBuilder updateCmd = new ProcessBuilder("apt-get", "update", "-y");
                Process updateProcess = updateCmd.start();
                int updateExitCode = updateProcess.waitFor();
                logger.log("apt-get update exit code: " + updateExitCode);
                
                ProcessBuilder installCmd = new ProcessBuilder("apt-get", "install", "-y", "openjdk-17-jdk");
                Process installProcess = installCmd.start();
                int installExitCode = installProcess.waitFor();
                logger.log("OpenJDK installation exit code: " + installExitCode);
                
                if (installExitCode == 0) {
                    logger.log("OpenJDK 17 JDK installed successfully");
                    
                    // Set JAVA_HOME environment
                    String javaHome = "/usr/lib/jvm/java-17-openjdk-amd64";
                    File javaHomeDir = new File(javaHome);
                    if (!javaHomeDir.exists()) {
                        javaHome = "/usr/lib/jvm/java-17-openjdk";
                    }
                    
                    logger.log("Setting JAVA_HOME to: " + javaHome);
                    System.setProperty("java.home", javaHome);
                }
                
            } catch (Exception e) {
                logger.log("Error during JDK installation: " + e.getMessage());
                logger.log("Attempting to continue without JDK installation...");
            }
        } catch (Exception e) {
            logger.log("Error while configuring Java environment: " + e.getMessage());
        }
    }
}