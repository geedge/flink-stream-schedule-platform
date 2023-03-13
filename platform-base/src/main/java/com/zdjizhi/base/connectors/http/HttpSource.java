package com.zdjizhi.base.connectors.http;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.json.JSONObject;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.base.Joiner;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.common.KnowledgeConstant;
import com.zdjizhi.base.common.KnowledgeLog;
import com.zdjizhi.base.function.source.RichHttpSourceFunction;
import com.zdjizhi.base.utils.CommonConfigurations;
import com.zdjizhi.base.utils.HdfsUtils;
import com.zdjizhi.base.utils.HttpClientUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

public class HttpSource extends RichHttpSourceFunction<Map<String, byte[]>> {

    private static final Logger logger = LoggerFactory.getLogger(HttpSource.class);

    private static final String KNOWLEDGE_EXPR;

    static {
        String IPV4_FILENAME = CommonConfig.IPV4_FILENAME;
        String ASNV4_FILENAME = CommonConfig.ASNV4_FILENAME;
        String ASNV6_FILENAME = CommonConfig.ASNV6_FILENAME;
        String WEBSKT_FILENAME = CommonConfig.WEBSKT_FILENAME;
        String DNS_FILENAME = CommonConfig.DNS_FILENAME;
        String ICP_FILENAME = CommonConfig.ICP_FILENAME;
        String LINK_FILENAME = CommonConfig.LINK_FILENAME;
        String ISP_FILENAME = CommonConfig.ISP_FILENAME;
        String IDC_RENTER_FILENAME = CommonConfig.IDC_RENTER_FILENAME;
        String INTERNAL_IP_FILENAME = CommonConfig.INTERNAL_IP_FILENAME;
        String APPSKT_FILENAME = CommonConfig.APPSKT_FILENAME;
        String WHOIS_FILENAME = CommonConfig.WHOIS_FILENAME;
        String IOC_FILENAME = CommonConfig.IOC_FILENAME;
        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && (@.name in ['");
        sb.append(IPV4_FILENAME);
        sb.append("','");
        sb.append(ASNV4_FILENAME);
        sb.append("','");
        sb.append(ASNV6_FILENAME);
        sb.append("','");
        sb.append(WEBSKT_FILENAME);
        sb.append("','");
        sb.append(DNS_FILENAME);
        sb.append("','");
        sb.append(ICP_FILENAME);
        sb.append("','");
        sb.append(LINK_FILENAME);
        sb.append("','");
        sb.append(ISP_FILENAME);
        sb.append("','");
        sb.append(IDC_RENTER_FILENAME);
        sb.append("','");
        sb.append(INTERNAL_IP_FILENAME);
        sb.append("','");
        sb.append(APPSKT_FILENAME);
        sb.append("','");
        sb.append(WHOIS_FILENAME);
        sb.append("'] || @.type in ['ip_user_defined','domain_user_defined','app_user_defined']))].['name','sha256','format','path','type']");
        KNOWLEDGE_EXPR = sb.toString();
    }

    private Properties nacosProperties;

    private String NACOS_DATA_ID;

    private String NACOS_GROUP;

    private long NACOS_READ_TIMEOUT;

    private static String STORE_PATH;

    private ConfigService configService;

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(List.class, KnowledgeLog.class);
    private static Map<String, String> updateMap = new HashMap<>();
    private static HashMap<String, byte[]> knowledgeFileCache;

    private boolean isRunning = true;


    public HttpSource(Properties nacosProperties, String NACOS_DATA_ID, String NACOS_GROUP, long NACOS_READ_TIMEOUT, String storePath) {
        this.nacosProperties = nacosProperties;
        this.NACOS_DATA_ID = NACOS_DATA_ID;
        this.NACOS_GROUP = NACOS_GROUP;
        this.NACOS_READ_TIMEOUT = NACOS_READ_TIMEOUT;
        this.STORE_PATH = storePath;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化元数据缓存
        updateMap = new HashMap<>(16);
        //初始化定位库缓存
        knowledgeFileCache = new HashMap<>(16);
        logger.info("连接nacos：" + nacosProperties.getProperty(PropertyKeyConst.SERVER_ADDR));
        configService = NacosFactory.createConfigService(nacosProperties);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {

        String config = configService.getConfig(NACOS_DATA_ID, NACOS_GROUP, NACOS_READ_TIMEOUT);
        logger.info( "receive config from nacos:" + config);
        if (StringUtil.isNotBlank(config)) {
            ArrayList<Object> metaList= JsonPath.parse(config).read(KNOWLEDGE_EXPR);
            loadKnowledge(metaList);
            ctx.collect(knowledgeFileCache);
        }

        configService.addListener(NACOS_DATA_ID, NACOS_GROUP, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configMsg) {
                try {
                    logger.info("receive update config:" + configMsg);
                    if (StringUtil.isNotBlank(configMsg)) {
                        ArrayList<Object> metaList = JsonPath.parse(configMsg).read(KNOWLEDGE_EXPR);
                        if (metaList.size() >= 1) {
                            for (Object metadata : metaList) {
                                JSONObject knowledgeJson = new JSONObject(metadata, false, true);
                                String fileName = Joiner.on(CommonConfig.LOCATION_SEPARATOR).useForNull("").join(knowledgeJson.getStr("name"),
                                        knowledgeJson.getStr("format"));
                                String sha256 = knowledgeJson.getStr("sha256");
                                String filePath = knowledgeJson.getStr("path");
                                String type = knowledgeJson.getStr("type");
                                List<String> tagTypes = new ArrayList<>(3);
                                tagTypes.add("ip_user_defined");
                                tagTypes.add("domain_user_defined");
                                tagTypes.add("app_user_defined");
                                if (tagTypes.contains(type)) {
                                    fileName = type + "," + fileName;
                                }
                                if (!sha256.equals(updateMap.get(fileName))) {
                                    updateMap.put(fileName, sha256);
                                    updateKnowledge(fileName, filePath);
                                }

                            }
                            ctx.collect(knowledgeFileCache);
                        }
                    }
                } catch (Exception e) {
                    logger.error("监听nacos配置失败", e);
                }
                System.out.println(configMsg);
            }
        });

        while (isRunning) {
            Thread.sleep(10000);
        }

    }

    private void loadKnowledge(ArrayList<Object> metaList) {
        InputStream inputStream = null;
        try {
            if (metaList.size() >= 1) {
                for (Object metadata : metaList) {
                    JSONObject knowledgeJson = new JSONObject(metadata, false, true);
                    String fileName = Joiner.on(CommonConfig.LOCATION_SEPARATOR).useForNull("").join(knowledgeJson.getStr("name"),
                            knowledgeJson.getStr("format"));
                    String sha256 = knowledgeJson.getStr("sha256");
                    String filePath = knowledgeJson.getStr("path");
                    String type = knowledgeJson.getStr("type");
                    List<String> tagTypes = new ArrayList<>(3);
                    tagTypes.add("ip_user_defined");
                    tagTypes.add("domain_user_defined");
                    tagTypes.add("app_user_defined");
                    if (tagTypes.contains(type)) {
                        fileName = type + "," + fileName;
                    }
                    Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
                    HttpClientUtils httpClientUtils = new HttpClientUtils();
                    inputStream = httpClientUtils.httpGetInputStream(filePath, 3000, header);
                    updateMap.put(fileName, sha256);
                    knowledgeFileCache.put(fileName, IOUtils.toByteArray(inputStream));
                }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }


    private void updateKnowledge(String fileName, String filePath) {
        InputStream inputStream = null;
        FileOutputStream outputStream = null;
        try {
            Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            inputStream = httpClientUtils.httpGetInputStream(filePath, 3000, header);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            HdfsUtils.uploadFileByBytes(CommonConfig.HDFS_PATH + fileName, bytes);
            knowledgeFileCache.put(fileName, bytes);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }




    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
