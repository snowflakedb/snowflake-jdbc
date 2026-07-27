package com.snowflake.wif.gcp;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.snowflake.wif.common.WifTestHelper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static java.net.URLDecoder.*;
import static java.util.stream.Collectors.*;

public class WifGcpFunctionE2e implements HttpFunction {
    
    private static final Logger logger = Logger.getLogger(WifGcpFunctionE2e.class.getName());

    private static class GcpLogger implements WifTestHelper.WifLogger {
        private final Logger logger;
        
        public GcpLogger(Logger logger) {
            this.logger = logger;
        }
        
        @Override
        public void log(String message) {
            logger.info(message);
        }
    }

    private static String createGcpResponseMessage(int mavenExitCode, String additionalInfo) {
        StringBuilder response = new StringBuilder();
        response.append("=== WIF GCP Function E2E Test Results ===\n");
        response.append(WifTestHelper.createMavenResultMessage(mavenExitCode));
        
        if (additionalInfo != null && !additionalInfo.trim().isEmpty()) {
            response.append("\n\nAdditional Info:\n");
            response.append(additionalInfo);
        }
        
        response.append("\n\nFunction Runtime: Google Cloud Functions (Java 17)");
        response.append("\nExecution Environment: GCP");
        
        return response.toString();
    }

    private static void performGcpCleanup(String workingDirectory, WifTestHelper.WifLogger logger) {
        logger.log("Performing GCP-specific cleanup...");
        
        try {
            WifTestHelper.cleanupWorkingDirectory(workingDirectory, logger);
            logger.log("GCP cleanup completed successfully");
        } catch (Exception e) {
            logger.log("Warning: GCP cleanup encountered issues: " + e.getMessage());
        }
    }

    @Override
    public void service(HttpRequest request, HttpResponse response) throws IOException {
        WifTestHelper.WifLogger wifLogger = new GcpLogger(logger);
        String workingDirectory = null;
        
        try {
            wifLogger.log("=== WIF GCP Function E2E started ===");
            Map<String, String> queryParameters = extractQueryParameters(request);
            String branch = queryParameters.get("BRANCH");
            String tarballUrl = WifTestHelper.buildTarballUrl(branch);

            // Download and extract to timestamp-based directory
            workingDirectory = WifTestHelper.downloadAndExtractRepository(tarballUrl, wifLogger);
            String repoFolderPath = WifTestHelper.findRepositoryFolder(workingDirectory);
            WifTestHelper.makeExecutable(repoFolderPath, wifLogger);
            
            int mavenExitCode = WifTestHelper.executeMavenBuild(repoFolderPath, System.getProperty("java.io.tmpdir"), wifLogger, queryParameters);

            createResponse(response, mavenExitCode, wifLogger);
        } catch (Exception e) {
            wifLogger.log("Error occurred: " + e.getMessage());
            if (e.getCause() != null) {
                wifLogger.log("Caused by: " + e.getCause().getMessage());
            }
            createErrorResponse(response, 500, "Error: " + e.getMessage());
        } finally {
            performGcpCleanup(workingDirectory, wifLogger);
        }
    }
    
    private Map<String, String> extractQueryParameters(HttpRequest request) throws IOException {
        Map<String, String> params = new HashMap<>();
        
        // Handle GET request query parameters
        Map<String, List<String>> queryParams = request.getQueryParameters();
        for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                params.put(entry.getKey(), entry.getValue().get(0));
            }
        }
        
        // Handle POST request form data
        if ("POST".equalsIgnoreCase(request.getMethod())) {
            String contentType = request.getContentType().orElse("");
            if (contentType.contains("application/x-www-form-urlencoded")) {
                String body = request.getReader().lines().collect(joining("\n"));
                if (body != null && !body.trim().isEmpty()) {
                    String[] pairs = body.split("&");
                    for (String pair : pairs) {
                        String[] keyValue = pair.split("=", 2);
                        if (keyValue.length == 2) {
                            try {
                                String key = decode(keyValue[0], "UTF-8");
                                String value = decode(keyValue[1], "UTF-8");
                                params.put(key, value);
                            } catch (Exception e) {
                                // Log but continue with other parameters
                                logger.warning("Failed to decode parameter: " + pair);
                            }
                        }
                    }
                }
            }
        }
        
        return params;
    }
    
    private void createResponse(HttpResponse response, int mavenExitCode, WifTestHelper.WifLogger logger) throws IOException {
        String responseBody = createGcpResponseMessage(mavenExitCode, "GCP Function execution completed");
        
        logger.log("Maven build completed with exit code: " + mavenExitCode);
        
        if (mavenExitCode == 0) {
            response.setStatusCode(200);
            logger.log("Returning success response");
        } else {
            response.setStatusCode(500);
            logger.log("Returning error response due to Maven build failure");
        }
        
        response.setContentType("text/plain; charset=utf-8");
        try (BufferedWriter writer = response.getWriter()) {
            writer.write(responseBody);
        }
    }

    private void createErrorResponse(HttpResponse response, int statusCode, String message) throws IOException {
        response.setStatusCode(statusCode);
        response.setContentType("text/plain; charset=utf-8");
        try (BufferedWriter writer = response.getWriter()) {
            writer.write(message);
        }
    }
}
