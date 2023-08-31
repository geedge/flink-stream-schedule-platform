package com.zdjizhi.base.connectors.http;

import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.function.source.RichHttpSourceFunction;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.base.utils.HttpClientUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

public class HttpSource extends RichHttpSourceFunction<Map<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(HttpSource.class);

    private final Properties nacosProperties;
    private final String dataId;
    private final String group;
    private final long readTimeout;
    private final String KNOWLEDGE_EXPR;

    private ConfigService configService;

    private static Map<String, String> knowLedgeFileSha256 = new HashMap<>();

    private boolean isRunning = true;


    public HttpSource(String knowledgeExpression) {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR,CommonConfig.NACOS_SERVER_ADDR);
        properties.setProperty(PropertyKeyConst.USERNAME, CommonConfig.NACOS_USERNAME);
        properties.setProperty(PropertyKeyConst.PASSWORD, CommonConfig.NACOS_PASSWORD);
        if (!StringUtils.isEmpty(CommonConfig.NACOS_NAMESPACE)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, CommonConfig.NACOS_NAMESPACE);
        }
        this.nacosProperties = properties;
        this.dataId = CommonConfig.NACOS_DATA_ID;
        this.group = CommonConfig.NACOS_GROUP;
        this.readTimeout = CommonConfig.NACOS_READ_TIMEOUT;
        this.KNOWLEDGE_EXPR = knowledgeExpression;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化元数据缓存
        knowLedgeFileSha256 = new HashMap<>(16);
        //初始化定位库缓存
        logger.info("connect nacos：" + nacosProperties.getProperty(PropertyKeyConst.SERVER_ADDR));
        configService = NacosFactory.createConfigService(nacosProperties);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        String nacosInfo = configService.getConfig(dataId, group, readTimeout);
        ctx.collect(processNacosInfo(nacosInfo));

        configService.addListener(dataId, group, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String nacosInfo) {
                ctx.collect(processNacosInfo(nacosInfo));
            }
        });

        System.out.println("listening nacos config");
        while (isRunning) {
            Thread.sleep(10000);
        }
    }

    private HashMap<String, String> processNacosInfo(String nacosInfo) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentDate = formatter.format(new Date());
        System.out.println(currentDate + ": receive nacos config:" + nacosInfo);
        ArrayList<Map<String, Object>> metaList = JsonPath.parse(nacosInfo).read(KNOWLEDGE_EXPR);
        System.out.println("resolve " + metaList.size() + " metadata from the config info pushed by nacos");
        return loadKnowledge(metaList);
    }

    private HashMap<String, String> loadKnowledge(ArrayList<Map<String, Object>> metaList) {
        HashMap<String, String> updateMap = new HashMap<>(16);

        List<String> tagTypes = new ArrayList<>(3);
        tagTypes.add(CommonInternalConfig.IP_TAG_TYPE);
        tagTypes.add(CommonInternalConfig.DOMAIN_TAG_TYPE);
        tagTypes.add(CommonInternalConfig.APP_TAG_TYPE);

        for (Map<String, Object> metadata : metaList) {
            String name = (String) metadata.get("name");
            String format = (String) metadata.get("format");
            String sha256 = (String) metadata.get("sha256");
            String fileUrl = (String) metadata.get("path");
            String type = (String) metadata.get("type");
            int isValid = (Integer) metadata.get("isValid");

            String fileName = type + "." + format;
            if (tagTypes.contains(type)) {
                fileName = type + "," + name + "." + format;
            }
            if (isValid != 1) {
                sha256 = CommonInternalConfig.NOT_EXIST_SHA256;
            }

            try {
                if (!knowLedgeFileSha256.containsKey(fileName)) {
                    byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                    if (checkFile(sha256, fileName)) {
                        FileUtils.uploadFileByBytes(fileName, downloadBytes);
                        updateMap.put(fileName, sha256);
                    }
                } else if (!knowLedgeFileSha256.get(fileName).equals(sha256)) {
                    byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                    FileUtils.uploadFileByBytes(fileName, downloadBytes);
                    updateMap.put(fileName, sha256);
                }

                if (tagTypes.contains(type)) {
                    byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                    FileUtils.uploadFileByBytes(fileName, downloadBytes);
                    updateMap.put(fileName, sha256);
                }

                knowLedgeFileSha256.put(fileName, sha256);
            } catch (IOException e) {
                System.out.println("process file " + fileName + " error, error message:" + e.getMessage());
                e.printStackTrace();
            }
        }
        return updateMap;
    }

    private boolean checkFile(String sha256, String fileName) {
        boolean isNeedUpdate = false;
        long start = System.currentTimeMillis();
        byte[] cacheBytes = FileUtils.getFileBytes(fileName);
        String cacheFileSha256 = DigestUtil.sha256Hex(cacheBytes);
        if (!sha256.equals(cacheFileSha256)) {
            isNeedUpdate = true;
        }
        long end = System.currentTimeMillis();
        System.out.println("knowledge file " + fileName + " check finished, is need update: " + isNeedUpdate + ", speed " + (end - start) + " ms");
        return isNeedUpdate;
    }

    private byte[] downloadFile(String fileUrl) {
        InputStream inputStream = null;
        byte[] content = new byte[0];
        try {
            long start = System.currentTimeMillis();
            Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            inputStream = httpClientUtils.httpGetInputStream(fileUrl, 3000, header);
            content = IOUtils.toByteArray(inputStream);
            long end = System.currentTimeMillis();
            System.out.println("download file " + fileUrl + " finished, speed " + (end - start) + " ms");
        } catch (IOException e) {
            System.out.println("download file " + fileUrl + " error: " + e.getMessage());
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return content;
    }

    private byte[] downloadFile(String fileUrl, String sha256, int isValid) {
        byte[] content = new byte[0];
        if (isValid != 1) {
            return content;
        }
        String downloadFileSha256;
        int downloadCount = 0;
        do {
            content = downloadFile(fileUrl);
            downloadFileSha256 = DigestUtil.sha256Hex(content);
            downloadCount++;
        } while (!downloadFileSha256.equals(sha256) && downloadCount < CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER);
        if (downloadCount >= CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER) {
            System.out.println("warning: file url: " + fileUrl + " download more than specified number of times");
        }
        return content;
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
