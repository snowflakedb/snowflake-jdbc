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

            String branch = request.getQueryParameters().get("BRANCH");
            String tarballUrl = WifTestHelper.buildTarballUrl(branch);

            // Download and extract to timestamp-based directory
            workingDirectory = WifTestHelper.downloadAndExtractRepository(tarballUrl, logger);
            String repoFolderPath = WifTestHelper.findRepositoryFolder(workingDirectory);
            WifTestHelper.makeExecutable(repoFolderPath, logger);
            int mavenExitCode = WifTestHelper.executeMavenBuild(repoFolderPath, System.getProperty("java.io.tmpdir"), logger, request.getQueryParameters());

            return createResponse(request, mavenExitCode);
        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage()).build();
        } finally {
            WifTestHelper.cleanupWorkingDirectory(workingDirectory, logger);
        }
    }
    
    private HttpResponseMessage createResponse(HttpRequestMessage<Optional<String>> request, int mavenExitCode) {
        String responseBody = WifTestHelper.createMavenResultMessage(mavenExitCode);
        
        if (mavenExitCode == 0) {
            return request.createResponseBuilder(HttpStatus.OK).body(responseBody).build();
        } else {
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(responseBody).build();
        }
    }
}