package com.zdjizhi.etl.knowledge;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.etl.utils.IPUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class NacosManager {

    private final String dataId = CommonConfig.NACOS_DATA_ID;
    private final String group = CommonConfig.NACOS_GROUP;
    private final Integer readTimeout = CommonConfig.NACOS_READ_TIMEOUT;

    private KnowledgeManager knowledgeManager;

    private final static AtomicBoolean ASYNC_FLUSH_RUNNING = new AtomicBoolean(false);

    private ConfigService configService = null;

    public void startAsyncListen() {
        if (ASYNC_FLUSH_RUNNING.compareAndSet(false, true)) {
            knowledgeManager = new KnowledgeManager();
            try {
                if (CommonInternalConfig.TEST_RUNNING == 1) {
                    // test running
                    try {
                        knowledgeManager.loadFromLocal();
                        IPUtils.load();
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
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
            }
        }
    }

    private void createService() throws NacosException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, CommonConfig.NACOS_SERVER_ADDR);
        properties.setProperty(PropertyKeyConst.USERNAME, CommonConfig.NACOS_USERNAME);
        properties.setProperty(PropertyKeyConst.PASSWORD, CommonConfig.NACOS_PASSWORD);
        if (!StringUtils.isEmpty(CommonConfig.NACOS_NAMESPACE)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, CommonConfig.NACOS_NAMESPACE);
        }
        configService = NacosFactory.createConfigService(properties);
    }

    private String getNacosInfo() throws NacosException {
        return configService.getConfig(dataId, group, readTimeout);
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
        configService.addListener(dataId, group, listener);
    }


}
