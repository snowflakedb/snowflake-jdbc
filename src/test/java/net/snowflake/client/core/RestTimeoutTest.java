package net.snowflake.client.core;

import net.snowflake.client.category.TestCategoryCore;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.atLeast;

@Category(TestCategoryCore.class)
public class RestTimeoutTest {

    private void verifyTimeouts(
        MockedStatic<HttpUtil> mock,
        int retryTimeout,
        int authTimeout,
        int socketTimeout,
        int retryCount
    ) {
        mock.verify(
            () -> HttpUtil.executeGeneralRequest(
                    any(),
                    eq(retryTimeout),
                    eq(authTimeout),
                    eq(socketTimeout),
                    eq(retryCount),
                    any(HttpClientSettingsKey.class)
            )
        );
    }

    private void mockRequest(
        MockedStatic<HttpUtil> mock,
        String body
    ) {
        mock
                .when(
                        () -> HttpUtil.executeGeneralRequest(
                                any(),
                                anyInt(),
                                anyInt(),
                                anyInt(),
                                anyInt(),
                                any(HttpClientSettingsKey.class)
                        )
                )
                .thenReturn(body);
    }

    @Test
    public void testSfSessionQueryStatusTimeouts() {
        try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class)) {
            mockRequest(mockedHttpUtil,"{ \"success\": true }");

            mockedHttpUtil
                .when(
                    () -> HttpUtil.getSocketTimeout()
                )
                .thenReturn(Duration.ofMillis(500));

            try {
                SFSession session = new SFSession();
                session.addProperty(SFSessionProperty.SERVER_URL, "www.example.com");
                session.getQueryStatusV2("some_id");
                verifyTimeouts(mockedHttpUtil, 300, 0, 500, 7);
            } catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testSfSessionHeartbeatTimeouts() {
        try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class)) {
            mockRequest(mockedHttpUtil,"{ \"success\": true }");
            mockedHttpUtil
                    .when(
                            () -> HttpUtil.getSocketTimeout()
                    )
                    .thenReturn(Duration.ofMillis(500));

            try {
                SFSession session = new SFSession();
                session.addProperty(SFSessionProperty.SERVER_URL, "www.example.com");
                session.open();
                session.heartbeat();
                verifyTimeouts(mockedHttpUtil, 300, 0, 500, 0);
            } catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}

