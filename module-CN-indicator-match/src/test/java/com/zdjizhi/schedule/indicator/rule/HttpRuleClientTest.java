package com.zdjizhi.schedule.indicator.rule;

import com.zdjizhi.base.rule.RuleClient;
import com.zdjizhi.base.rule.info.RuleInfo;
import org.apache.commons.io.IOUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HttpRuleClientTest {

    @Mock
    private HttpClient mockClient;

    @Mock
    private HttpGet mockHttpGet;

    @Mock
    private HttpResponse mockResponse;

    private MockedStatic<EntityUtils> mockedEntityUtils;

    @Mock
    private AtomicLong mockVersion;

    private final String incURL = "https://localhost/inc";

    private final String fullURL = "https://localhost/";

    private String content;

    private HttpRuleClient client;

    @BeforeEach
    void setUp() throws IOException {
        client = new HttpRuleClient(mockClient, mockVersion, new ObjectMapper(), incURL, fullURL);

        try (InputStream is = getClass().getResourceAsStream("/data/rule.json")) {
            assert is != null;
            content = String.join("", IOUtils.readLines(is));
        }

        mockedEntityUtils = mockStatic(EntityUtils.class);
    }


    @Test
    void testGet() throws IOException {

        mockedEntityUtils.when(() -> EntityUtils.toString(any(), anyString()))
                .thenReturn(content);

        doReturn(mockResponse).when(mockClient).execute(any());

        assertEquals(content, EntityUtils.toString(mockResponse.getEntity(), "UTF-8"));

        RuleClient.Updatable<Long, RuleInfo> updatable = client.get();

        assertEquals(2, updatable.getAddList().size());
    }

}