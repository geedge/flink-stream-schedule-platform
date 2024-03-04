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

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2023/12/25 16:55
 */
public class MetadataListenerUtils {

    private static final Logger logger = LoggerFactory.getLogger(MetadataListenerUtils.class);

    private final static AtomicBoolean ASYNC_LISTEN_RUNNING = new AtomicBoolean(false);
    private final static CountDownLatch ASYNC_WAIT = new CountDownLatch(1);

    private static int threadCount = 0;

    private static FixedRateFetcher fixedRateFetcher;

    public synchronized static void onListen(Configuration configuration) throws Exception {
        threadCount++;
        if (ASYNC_LISTEN_RUNNING.compareAndSet(false, true)) {
            try {
                FetchRunnable fetchRunnable = new FetchRunnable(configuration);
                fetchRunnable.run();
                fixedRateFetcher = new FixedRateFetcher(fetchRunnable, configuration.get(CommonConfig.KNOWLEDGE_URL_INTERVAL_MS)) {
                    @Override
                    public void callback(@Nullable RuntimeException e) {
                        if (e != null) {
                            logger.error("metadata listen task run failed", e);
                        }
                    }
                };
                fixedRateFetcher.open();
            } catch (Exception e) {
                logger.error("start metadata listener error", e);
                if (fixedRateFetcher != null) {
                    fixedRateFetcher.close();
                }
                throw new RuntimeException(e);
            } finally {
                ASYNC_WAIT.countDown();
            }
        }
        ASYNC_WAIT.await();
    }

    public synchronized static void close() throws Exception {
        threadCount--;
        if (threadCount == 0) {
            fixedRateFetcher.close();
            KnowledgeManager.close();
            ASYNC_LISTEN_RUNNING.set(false);
            logger.info("close metadata listener and knowledge manager");
        }
    }

    private static final class FetchRunnable implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(FetchRunnable.class);

        private final String url;

        private final Configuration configuration;

        private final KnowledgeManager knowledgeManager;

        private String cache = "";

        private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createMinimal();

        public FetchRunnable(Configuration configuration) {
            this.configuration = configuration;
            knowledgeManager = new KnowledgeManager(configuration);
            url = getUrl();
        }

        private String getUrl() {
            return "http://"
                    + configuration.get(CommonConfig.GATEWAY_HOST)
                    + ":"
                    + configuration.get(CommonConfig.GATEWAY_PORT)
                    + configuration.get(CommonConfig.KNOWLEDGE_PATH);
        }

        @Override
        public void run() {
            final HttpGet httpGet = new HttpGet(url);
            httpGet.addHeader("Accept", "application/json");
            try {
                CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String content = EntityUtils.toString(entity, "UTF-8");
                    KnowledgeResponse knowledgeResponse = JSON.parseObject(content, KnowledgeResponse.class);
                    if (ifProcess(knowledgeResponse.getData())) {
                        knowledgeManager.processNacosInfo(knowledgeResponse.getData());
                    }
                }
            } catch (Exception e) {
                logger.error("fetch knowledge metadata error", e);
            }
        }

        private boolean ifProcess(String data) {
            if (!cache.equals(data)) {
                cache = data;
                return true;
            }
            return false;
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
