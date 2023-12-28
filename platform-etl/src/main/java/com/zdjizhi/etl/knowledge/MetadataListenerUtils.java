package com.zdjizhi.etl.knowledge;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.rule.async.FixedRateFetcher;
import com.zdjizhi.etl.common.CommonConfig;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2023/12/25 16:55
 */
public class MetadataListenerUtils {

    private final static AtomicBoolean ASYNC_LISTEN_RUNNING = new AtomicBoolean(false);

    public static void onListen(Configuration configuration) {
        if (ASYNC_LISTEN_RUNNING.compareAndSet(false, true)) {
            FixedRateFetcher fixedRateFetcher = new FixedRateFetcher(new FetchRunnable(configuration), configuration.get(CommonConfig.KNOWLEDGE_URL_INTERVAL_MS));
            fixedRateFetcher.open();
        }
    }

    private static final class FetchRunnable implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(FetchRunnable.class);

        private final String url;

        private final Configuration configuration;

        private final KnowledgeManager knowledgeManager;

        private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createMinimal();

        public FetchRunnable(Configuration configuration) {
            this.configuration = configuration;
            knowledgeManager = new KnowledgeManager(configuration);
            url = configuration.get(CommonConfig.KNOWLEDGE_URL) + "/v1/knowledge_base";
        }

        @Override
        public void run() {
            final HttpGet httpGet = new HttpGet(url);
            httpGet.addHeader("Accept", "application/json");
            httpGet.addHeader("Cn-Authorization", configuration.get(CommonConfig.KNOWLEDGE_URL_TOKEN));
            try {
                CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String content = EntityUtils.toString(entity, "UTF-8");
                    KnowledgeResponse knowledgeResponse = JSON.parseObject(content, KnowledgeResponse.class);
                    knowledgeManager.processNacosInfo(knowledgeResponse.getData());
                }
            } catch (IOException e) {
                logger.error("fetch knowledge metadata error", e);
            }
        }
    }

    @Data
    private static final class KnowledgeResponse {
        private int status;
        private boolean success;
        private String message;
        private String data;
    }
}
