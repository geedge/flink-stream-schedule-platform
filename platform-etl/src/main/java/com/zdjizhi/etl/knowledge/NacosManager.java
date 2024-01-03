package com.zdjizhi.etl.knowledge;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.etl.utils.ip.IPUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

@Deprecated
public class NacosManager {

    private final Logger logger = LoggerFactory.getLogger(NacosManager.class);

    private KnowledgeManager knowledgeManager;

    private final Configuration configuration;

    private final static AtomicBoolean ASYNC_LISTEN_RUNNING = new AtomicBoolean(false);
    private final static CountDownLatch ASYNC_WAIT = new CountDownLatch(1);

    private ConfigService configService = null;

    public NacosManager(Configuration configuration) {
        this.configuration = configuration;
    }

    public void startAsyncListen() throws InterruptedException {
        if (ASYNC_LISTEN_RUNNING.compareAndSet(false, true)) {
            try {
                knowledgeManager = new KnowledgeManager(configuration);
                if (configuration.get(CommonInternalConfig.TEST_RUNNING) == 1) {
                    // test running
                    try {
                        knowledgeManager.loadFromLocal();
                        IPUtils.load();
                    } catch (Exception e) {
                        logger.error("init from local failed :" + e.getMessage(), e);
                        throw new RuntimeException("init from local failed :" + e.getMessage());
                    }
                } else {
                    createService();
                    String nacosInfo = getNacosInfo();
                    knowledgeManager.processNacosInfo(nacosInfo);
                    listenNacos();
                }
            } catch (NacosException e) {
                throw new RuntimeException(e);
            } finally {
                ASYNC_WAIT.countDown();
            }
        }
        ASYNC_WAIT.await();
    }

    private void createService() throws NacosException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, configuration.get(CommonConfig.NACOS_SERVER_ADDR));
        properties.setProperty(PropertyKeyConst.USERNAME, configuration.get(CommonConfig.NACOS_USERNAME));
        properties.setProperty(PropertyKeyConst.PASSWORD, configuration.get(CommonConfig.NACOS_PASSWORD));
        if (!StringUtils.isEmpty(configuration.get(CommonConfig.NACOS_NAMESPACE))) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, configuration.get(CommonConfig.NACOS_NAMESPACE));
        }
        configService = NacosFactory.createConfigService(properties);
    }

    private String getNacosInfo() throws NacosException {
        return configService.getConfig(configuration.get(CommonConfig.NACOS_DATA_ID), configuration.get(CommonConfig.NACOS_GROUP), configuration.get(CommonConfig.NACOS_READ_TIMEOUT));
    }

    private void listenNacos() throws NacosException {
        Listener listener = new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String nacosInfo) {
                knowledgeManager.processNacosInfo(nacosInfo);
            }
        };
        configService.addListener(configuration.get(CommonConfig.NACOS_DATA_ID), configuration.get(CommonConfig.NACOS_GROUP), listener);
    }


}
