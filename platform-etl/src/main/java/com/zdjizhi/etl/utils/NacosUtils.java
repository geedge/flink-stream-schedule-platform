package com.zdjizhi.etl.utils;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.concurrent.Executor;

public class NacosUtils {

    private static final String dataId = CommonConfig.NACOS_DATA_ID;
    private static final String group = CommonConfig.NACOS_GROUP;
    private static final Integer readTimeout = CommonConfig.NACOS_READ_TIMEOUT;

    private static ConfigService configService = null;

    static {
        try {
            if (CommonInternalConfig.TEST_RUNNING == 1) {
                // test running
                try {
                    KnowledgeUtils.loadFromLocal();
                    IPUtils.load();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    throw new RuntimeException("init from local failed :" + e.getMessage());
                }
            } else {
                createService();
                String nacosInfo = getNacosInfo();
                KnowledgeUtils.processNacosInfo(nacosInfo);
                listenNacos();
            }
        } catch (NacosException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createService() throws NacosException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, CommonConfig.NACOS_SERVER_ADDR);
        properties.setProperty(PropertyKeyConst.USERNAME, CommonConfig.NACOS_USERNAME);
        properties.setProperty(PropertyKeyConst.PASSWORD, CommonConfig.NACOS_PASSWORD);
        if (!StringUtils.isEmpty(CommonConfig.NACOS_NAMESPACE)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, CommonConfig.NACOS_NAMESPACE);
        }
        configService = NacosFactory.createConfigService(properties);
    }

    private static String getNacosInfo() throws NacosException {
        return configService.getConfig(dataId, group, readTimeout);
    }

    private static void listenNacos() throws NacosException {
        configService.addListener(dataId, group, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String nacosInfo) {
                KnowledgeUtils.processNacosInfo(nacosInfo);
            }
        });
    }


}
