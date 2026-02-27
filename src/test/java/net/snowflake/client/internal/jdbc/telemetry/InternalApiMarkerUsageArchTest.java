package net.snowflake.client.internal.jdbc.telemetry;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.fasterxml.jackson.databind.JsonNode;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import net.snowflake.client.api.resultset.SnowflakeResultSetSerializable;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFBaseStatement;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFStatement;
import net.snowflake.client.internal.core.SessionUtil;
import net.snowflake.client.internal.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.internal.jdbc.SnowflakeResultSetSerializableV1;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class InternalApiMarkerUsageArchTest {
  private static final String INTERNAL_PACKAGE = "net.snowflake.client.internal..";

  private static final JavaClasses INTERNAL_CLASSES =
      new ClassFileImporter()
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
          .importPackages("net.snowflake.client.internal");

  @Test
  void internalClassesMustUseMarkerAwareTrackedApis() {
    noInternalCallsTo(SnowflakeConnectionImpl.class, "getHandler").check(INTERNAL_CLASSES);
    noInternalCallsTo(SnowflakeConnectionImpl.class, "getSFBaseSession").check(INTERNAL_CLASSES);
    noInternalCallsTo(SnowflakeConnectionImpl.class, "getSfSession").check(INTERNAL_CLASSES);

    noInternalCallsTo(SFSession.class, "open").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFSession.class, "close").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFSession.class, "getSessionToken").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFSession.class, "getTelemetryClient").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFSession.class, "getIdToken").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFSession.class, "getAccessToken").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFSession.class, "getMfaToken").check(INTERNAL_CLASSES);

    noInternalCallsTo(SFStatement.class, "getSFBaseSession").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFBaseSession.class, "getTelemetryClient").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFBaseSession.class, "close").check(INTERNAL_CLASSES);
    noInternalCallsTo(SFBaseStatement.class, "getSFBaseSession").check(INTERNAL_CLASSES);

    noInternalCallsTo(
            SessionUtil.class,
            "generateJWTToken",
            java.security.PrivateKey.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class)
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(
            SessionUtil.class,
            "generateJWTToken",
            java.security.PrivateKey.class,
            String.class,
            String.class,
            String.class,
            String.class)
        .check(INTERNAL_CLASSES);

    noInternalCallsToConstructor(
            SnowflakeFileTransferAgent.class, String.class, SFSession.class, SFStatement.class)
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(SnowflakeFileTransferAgent.class, "getFileTransferMetadatas")
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(SnowflakeFileTransferAgent.class, "getFileTransferMetadatas", JsonNode.class)
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(
            SnowflakeFileTransferAgent.class,
            "getFileTransferMetadatas",
            JsonNode.class,
            String.class)
        .check(INTERNAL_CLASSES);

    noInternalCallsTo(SnowflakeResultSetSerializableV1.class, "getResultStreamProvider")
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(SnowflakeResultSetSerializableV1.class, "getSFResultSetMetaData")
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(SnowflakeResultSetSerializableV1.class, "getSession").check(INTERNAL_CLASSES);
    noInternalCallsTo(
            SnowflakeResultSetSerializableV1.class,
            "create",
            JsonNode.class,
            SFBaseSession.class,
            SFBaseStatement.class)
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(
            SnowflakeResultSetSerializableV1.class,
            "create",
            JsonNode.class,
            SFBaseSession.class,
            SFBaseStatement.class,
            net.snowflake.client.internal.jdbc.ResultStreamProvider.class)
        .check(INTERNAL_CLASSES);
    noInternalCallsTo(
            SnowflakeResultSetSerializableV1.class,
            "getResultSet",
            SnowflakeResultSetSerializable.ResultSetRetrieveConfig.class)
        .check(INTERNAL_CLASSES);
  }

  private static ArchRule noInternalCallsTo(
      Class<?> ownerClass, String methodName, Class<?>... parameterTypes) {
    return noClasses()
        .that()
        .resideInAPackage(INTERNAL_PACKAGE)
        .and()
        .doNotHaveFullyQualifiedName(ownerClass.getName())
        .should()
        .callMethod(ownerClass, methodName, parameterTypes)
        .because(
            "internal call sites must use marker-aware overloads to avoid external telemetry false positives");
  }

  private static ArchRule noInternalCallsToConstructor(
      Class<?> ownerClass, Class<?>... parameterTypes) {
    return noClasses()
        .that()
        .resideInAPackage(INTERNAL_PACKAGE)
        .and()
        .doNotHaveFullyQualifiedName(ownerClass.getName())
        .should()
        .callConstructor(ownerClass, parameterTypes)
        .because(
            "internal call sites must use marker-aware overloads to avoid external telemetry false positives");
  }
}
