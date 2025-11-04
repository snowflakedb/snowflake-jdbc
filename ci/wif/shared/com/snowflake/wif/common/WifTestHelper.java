package com.snowflake.wif.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

/**
 * Helper class containing common functions for WIF E2E tests that can be reused
 * across different cloud function implementations (AWS Lambda, Azure Functions).
 */
public class WifTestHelper {

    private static String generateUniqueSessionId() {
        return System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public static void validateQueryParameters(Map<String, String> queryParams) {
        if (queryParams == null) {
            throw new IllegalArgumentException("Missing query parameters");
        }

        String wifHost = queryParams.get("SNOWFLAKE_TEST_WIF_HOST");
        String wifAccount = queryParams.get("SNOWFLAKE_TEST_WIF_ACCOUNT");
        String wifProvider = queryParams.get("SNOWFLAKE_TEST_WIF_PROVIDER");
        String branch = queryParams.get("BRANCH");

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

    public static String buildTarballUrl(String branch) {
        if (Pattern.matches("^PR-\\d+$", branch)) {
            String prNumber = branch.substring(3);
            return "https://github.com/snowflakedb/snowflake-jdbc/archive/refs/pull/" + prNumber + "/head.tar.gz";
        } else {
            return "https://github.com/snowflakedb/snowflake-jdbc/archive/refs/heads/" + branch + ".tar.gz";
        }
    }

    public static String findRepositoryFolder(String workingDirectory) {
        if (workingDirectory == null) {
            throw new RuntimeException("Working directory must be specified");
        }
        
        File workDir = new File(workingDirectory);
        File[] files = workDir.listFiles();
        
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && file.getName().startsWith("snowflake-jdbc-")) {
                    return file.getAbsolutePath();
                }
            }
        }
        
        throw new RuntimeException("Driver repository folder not found in: " + workingDirectory);
    }

    public static int executeMavenBuild(String repoFolderPath, String tempDirectory, WifLogger logger, Map<String, String> queryParams) {
        Process process = null;
        File mavenRepo = null;
        File workspace = null;
        File mavenHomeDir = null;
        try {
            // Use unique session ID to avoid conflicts with parallel test runs
            String sessionId = generateUniqueSessionId();
            mavenRepo = new File(tempDirectory, "maven-repo-" + sessionId);
            workspace = new File(tempDirectory, "workspace-" + sessionId);
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
                "-Dmaven.artifact.threads=4 " +
                "-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn " +
                "-Dnot-self-contained-jar " +
                "-Dmaven.test.failure.ignore=false " +
                "-Dmaven.compile.fork=false " +
                "-Dmaven.javadoc.skip=true " +
                "-Dmaven.source.skip=true " +
                "-Dcheckstyle.skip=true " +
                "-Dspotbugs.skip=true " +
                "-Denforcer.skip=true " +
                "-T 2C " +
                "test-compile verify --batch-mode --show-version --fail-fast --no-transfer-progress"
            );
            pb.redirectErrorStream(true);
            
            // Redirect all Maven directories to temp directory to avoid read-only filesystem issues
            String mavenHome = tempDirectory + "/maven-home-" + sessionId;
            mavenHomeDir = new File(mavenHome);
            mavenHomeDir.mkdirs();
            
            logger.log("Setting Maven home to: " + mavenHome);
            
            pb.environment().put("MAVEN_USER_HOME", mavenHome);
            pb.environment().put("HOME", tempDirectory);
            pb.environment().put("USER_HOME", tempDirectory);
            pb.environment().put("MAVEN_OPTS", "-Xmx2g -Xms512m -Duser.home=" + tempDirectory + " -Djava.io.tmpdir=" + tempDirectory);
            
            pb.environment().put("SNOWFLAKE_TEST_WIF_HOST", queryParams.get("SNOWFLAKE_TEST_WIF_HOST"));
            pb.environment().put("SNOWFLAKE_TEST_WIF_ACCOUNT", queryParams.get("SNOWFLAKE_TEST_WIF_ACCOUNT"));
            pb.environment().put("SNOWFLAKE_TEST_WIF_PROVIDER", queryParams.get("SNOWFLAKE_TEST_WIF_PROVIDER"));
            pb.environment().put("SF_ENABLE_EXPERIMENTAL_AUTHENTICATION", "true");
            pb.environment().put("IS_GCP_FUNCTION", queryParams.getOrDefault("IS_GCP_FUNCTION", "false"));

            process = pb.start();
            
            final InputStream processInputStream = process.getInputStream();
            Thread outputThread = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(processInputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        logger.log(line);
                    }
                } catch (IOException e) {
                    logger.log("Error reading Maven output: " + e.getMessage());
                }
            });
            outputThread.start();
            
            if (!process.waitFor(6, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                return -2; // timeout
            }
            return process.exitValue();
            
        } catch (Exception e) {
            logger.log("Maven build error: " + e.getMessage());
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
            
            // Clean up Maven temporary directories
            cleanupMavenTempDirectories(mavenRepo, workspace, mavenHomeDir, logger);
        }
    }
    
    private static void cleanupMavenTempDirectories(File mavenRepo, File workspace, File mavenHomeDir, WifLogger logger) {
        logger.log("Cleaning up Maven temporary directories...");
        
        cleanupDirectory(mavenRepo, "Maven repository", logger);
        cleanupDirectory(workspace, "workspace", logger);
        cleanupDirectory(mavenHomeDir, "Maven home", logger);
        
        logger.log("Maven temporary directories cleanup completed");
    }
    
    private static void cleanupDirectory(File directory, String description, WifLogger logger) {
        if (directory != null && directory.exists()) {
            try {
                ProcessBuilder pb = new ProcessBuilder("rm", "-rf", directory.getAbsolutePath());
                Process process = pb.start();
                int exitCode = process.waitFor();
                
                if (exitCode == 0) {
                    logger.log("Successfully cleaned up " + description + ": " + directory.getAbsolutePath());
                } else {
                    logger.log("Warning: Failed to clean up " + description + " (exit code " + exitCode + "): " + directory.getAbsolutePath());
                }
            } catch (Exception e) {
                logger.log("Warning: Exception during " + description + " cleanup: " + e.getMessage());
            }
        } else if (directory != null) {
            logger.log("Directory already cleaned or doesn't exist: " + directory.getAbsolutePath());
        }
    }
    
    /**
     * Makes mvnw and check_content.sh executable after downloading repository.
     * This ensures the Maven wrapper and CI scripts can be executed in cloud environments.
     * 
     * @param repoFolderPath Path to the repository folder
     * @param logger Logger for output
     */
    public static void makeExecutable(String repoFolderPath, WifLogger logger) {
        try {
            File mvnwFile = new File(repoFolderPath, "mvnw");
            if (mvnwFile.exists()) {
                boolean wasExecutable = mvnwFile.canExecute();
                boolean success = mvnwFile.setExecutable(true, false);
                
                logger.log(String.format(
                    "Found mvnw: %s (was executable: %s, set executable: %s, now executable: %s)",
                    mvnwFile.getAbsolutePath(),
                    wasExecutable,
                    success,
                    mvnwFile.canExecute()
                ));
                
                mvnwFile.setReadable(true, false);
            } else {
                logger.log("Warning: mvnw file not found at " + mvnwFile.getAbsolutePath());
            }
            
            File checkContentFile = new File(repoFolderPath, "ci/scripts/check_content.sh");
            if (checkContentFile.exists()) {
                boolean wasExecutable = checkContentFile.canExecute();
                boolean success = checkContentFile.setExecutable(true, false);
                
                logger.log(String.format(
                    "Found check_content.sh: %s (was executable: %s, set executable: %s, now executable: %s)",
                    checkContentFile.getAbsolutePath(),
                    wasExecutable,
                    success,
                    checkContentFile.canExecute()
                ));
                checkContentFile.setReadable(true, false);
            } else {
                logger.log("Warning: check_content.sh file not found at " + checkContentFile.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.log("Warning: Failed to fix file permissions: " + e.getMessage());
        }
    }

    public static void cleanupWorkingDirectory(String workingDirectory, WifLogger logger) {
        try {
            if (workingDirectory == null) {
                logger.log("No working directory to clean up");
                return;
            }
            
            File workDir = new File(workingDirectory);
            if (!workDir.exists()) {
                logger.log("Working directory does not exist: " + workingDirectory);
                return;
            }
            
            logger.log("Cleaning up working directory: " + workingDirectory);
            
            try {
                ProcessBuilder pb = new ProcessBuilder("rm", "-rf", workingDirectory);
                Process process = pb.start();
                int exitCode = process.waitFor();
                
                if (exitCode == 0) {
                    logger.log("Successfully cleaned up working directory");
                } else {
                    logger.log("rm -rf command failed with exit code: " + exitCode);
                }
                
            } catch (Exception e) {
                logger.log("rm -rf command failed: " + e.getMessage());
            }
            
            // Also clean up any truly old orphaned Maven directories (older than 1 hour)
            cleanupOrphanedMavenDirectories(logger);
            
        } catch (Exception e) {
            logger.log("Working directory cleanup warning: " + e.getMessage());
        }
    }
    
    /**
     * Clean up any orphaned Maven temporary directories that might have been left behind
     * from previous failed executions. This is a defensive cleanup measure that only
     * removes directories older than 1 hour to avoid conflicts with parallel test runs.
     */
    private static void cleanupOrphanedMavenDirectories(WifLogger logger) {
        try {
            String tempDir = System.getProperty("java.io.tmpdir");
            File tempDirectory = new File(tempDir);
            
            if (!tempDirectory.exists() || !tempDirectory.isDirectory()) {
                return;
            }
            
            logger.log("Scanning for old orphaned Maven directories in: " + tempDir);
            
            File[] files = tempDirectory.listFiles();
            if (files != null) {
                int cleanedCount = 0;
                long oneHourAgo = System.currentTimeMillis() - (60 * 60 * 1000); // 1 hour ago
                
                for (File file : files) {
                    if (file.isDirectory() && (
                        file.getName().startsWith("maven-repo-") ||
                        file.getName().startsWith("workspace-") ||
                        file.getName().startsWith("maven-home-") ||
                        file.getName().startsWith("wif-function-")
                    )) {
                        // Only clean up directories older than 1 hour to avoid interfering with parallel runs
                        if (file.lastModified() < oneHourAgo) {
                            cleanupDirectory(file, "old orphaned directory", logger);
                            cleanedCount++;
                        } else {
                            logger.log("Skipping recent directory (likely from parallel run): " + file.getName());
                        }
                    }
                }
                
                if (cleanedCount > 0) {
                    logger.log("Cleaned up " + cleanedCount + " old orphaned Maven/WIF directories");
                } else {
                    logger.log("No old orphaned Maven/WIF directories found");
                }
            }
            
        } catch (Exception e) {
            logger.log("Warning: Failed to clean up orphaned directories: " + e.getMessage());
        }
    }

    public static String downloadAndExtractRepository(String tarballUrl, WifLogger logger) throws IOException, InterruptedException {
        // Create unique session-based directory for complete isolation
        String sessionId = generateUniqueSessionId();
        String workDir = "/tmp/wif-function-" + sessionId;
        File workingDirectory = new File(workDir);

        if (!workingDirectory.exists()) {
            workingDirectory.mkdirs();
        }
        
        logger.log("Extracting to timestamp-based directory: " + workingDirectory.getAbsolutePath());
        logger.log("Downloading using Java HTTP client: " + tarballUrl);
        
        try {
            downloadAndExtractWithJava(tarballUrl, workingDirectory, logger);
            logger.log("Download and extraction completed successfully");
            return workingDirectory.getAbsolutePath();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to download and extract: " + e.getMessage(), e);
        }
    }

    public static void downloadAndExtractWithJava(String tarballUrl, File workingDirectory, WifLogger logger) throws IOException, InterruptedException {
        logger.log("Creating HTTP client...");
        
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .build();
            
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(tarballUrl))
            .timeout(Duration.ofMinutes(5))
            .header("User-Agent", "WIF-Function")
            .GET()
            .build();
            
        logger.log("Sending HTTP request...");
        
        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            
            if (response.statusCode() != 200) {
                throw new IOException("HTTP " + response.statusCode() + " when downloading " + tarballUrl);
            }
            
            logger.log("HTTP response received, extracting tar.gz...");
            
            try (InputStream responseStream = response.body();
                 GZIPInputStream gzipStream = new GZIPInputStream(responseStream);
                 TarArchiveInputStream tarStream = new TarArchiveInputStream(gzipStream)) {
                
                TarArchiveEntry entry;
                while ((entry = tarStream.getNextTarEntry()) != null) {
                    File outputFile = createOutputFile(workingDirectory, entry.getName());
                    
                    if (entry.isDirectory()) {
                        outputFile.mkdirs();
                    } else {
                        outputFile.getParentFile().mkdirs();
                        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                            tarStream.transferTo(fos);
                        }
                    }
                }
            }
            
            logger.log("Tar extraction completed");
            
        } catch (Exception e) {
            logger.log("Download/extract error: " + e.getMessage());
            throw new IOException("Failed to download and extract: " + e.getMessage(), e);
        }
    }

    private static File createOutputFile(File workingDirectory, String entryName) throws IOException {
        File outputFile = new File(workingDirectory, entryName);
        
        String workingDirCanonical = workingDirectory.getCanonicalPath();
        String outputFileCanonical = outputFile.getCanonicalPath();
        
        if (!outputFileCanonical.startsWith(workingDirCanonical + File.separator) && 
            !outputFileCanonical.equals(workingDirCanonical)) {
            throw new IOException("Archive entry '" + entryName + "' would extract outside the target directory: " + outputFileCanonical);
        }
        
        return outputFile;
    }

    public static String createMavenResultMessage(int mavenExitCode) {
        if (mavenExitCode == 0) {
            return "WIF tests completed successfully";
        } else {
            String mavenResult;
            String testResults;
            
            if (mavenExitCode == -2) {
                mavenResult = "TIMEOUT (exit code: " + mavenExitCode + ")";
                testResults = "Build timed out after 6 minutes";
            } else {
                mavenResult = "FAILED (exit code: " + mavenExitCode + ")";
                testResults = "Build or tests failed";
            }
            
            return String.format(
                    "MAVEN_RESULT=%s\nTEST_STATUS=%s",
                    mavenResult, testResults
            );
        }
    }

    public interface WifLogger {
        void log(String message);
    }
}
