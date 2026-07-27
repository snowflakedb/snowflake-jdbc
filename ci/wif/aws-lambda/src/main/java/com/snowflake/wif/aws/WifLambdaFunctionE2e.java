package com.snowflake.wif.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.wif.common.WifTestHelper;

import java.util.HashMap;
import java.util.Map;

public class WifLambdaFunctionE2e implements RequestHandler<Object, APIGatewayProxyResponseEvent> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static class LambdaLogger implements WifTestHelper.WifLogger {
        private final Context context;
        
        public LambdaLogger(Context context) {
            this.context = context;
        }
        
        @Override
        public void log(String message) {
            context.getLogger().log(message);
        }
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(Object input, Context context) {
        WifTestHelper.WifLogger logger = new LambdaLogger(context);
        String workingDirectory = null;
        
        try {
            logger.log("=== WIF AWS Lambda Function E2E started ===");
            logger.log("Input received: " + objectMapper.writeValueAsString(input));
            
            Map<String, String> queryParameters = extractQueryParameters(input, logger);
            
            WifTestHelper.validateQueryParameters(queryParameters);

            String branch = queryParameters.get("BRANCH");
            String tarballUrl = WifTestHelper.buildTarballUrl(branch);

            workingDirectory = WifTestHelper.downloadAndExtractRepository(tarballUrl, logger);
            String repoFolderPath = WifTestHelper.findRepositoryFolder(workingDirectory);
            WifTestHelper.makeExecutable(repoFolderPath, logger);
            int mavenExitCode = WifTestHelper.executeMavenBuild(repoFolderPath, System.getProperty("java.io.tmpdir"), logger, queryParameters);

            return createResponse(mavenExitCode);
        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            return createErrorResponse(500, "Error: " + e.getMessage());
        } finally {
            WifTestHelper.cleanupWorkingDirectory(workingDirectory, logger);
        }
    }
    
    private Map<String, String> extractQueryParameters(Object input, WifTestHelper.WifLogger logger) throws Exception {
        JsonNode inputNode = objectMapper.valueToTree(input);
        
        // Handle Lambda Function URL format (query parameters in queryStringParameters)
        if (inputNode.has("queryStringParameters")) {
            JsonNode queryParamsNode = inputNode.get("queryStringParameters");
            if (queryParamsNode != null && !queryParamsNode.isNull()) {
                logger.log("Processing Lambda Function URL with queryStringParameters");
                return objectMapper.convertValue(queryParamsNode, Map.class);
            }
        }
        
        // Handle direct invocation format (parameters at root level)
        if (inputNode.has("SNOWFLAKE_TEST_WIF_HOST")) {
            logger.log("Processing direct invocation with parameters at root level");
            return objectMapper.convertValue(inputNode, Map.class);
        }
        
        throw new IllegalArgumentException("Invalid input format: expected Lambda Function URL event or direct invocation with required parameters");
    }
    
    private APIGatewayProxyResponseEvent createResponse(int mavenExitCode) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "text/plain");
        response.setHeaders(headers);

        String responseBody = WifTestHelper.createMavenResultMessage(mavenExitCode);
        
        if (mavenExitCode == 0) {
            response.setStatusCode(200);
        } else {
            response.setStatusCode(500);
        }
        
        response.setBody(responseBody);
        return response;
    }

    private APIGatewayProxyResponseEvent createErrorResponse(int statusCode, String message) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "text/plain");
        response.setHeaders(headers);
        response.setStatusCode(statusCode);
        response.setBody(message);
        return response;
    }
}