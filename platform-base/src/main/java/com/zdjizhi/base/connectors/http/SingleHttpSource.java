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
import com.zdjizhi.base.common.KnowledgeLog;
import com.zdjizhi.base.function.source.RichHttpSourceFunction;
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
import java.util.*;
import java.util.concurrent.Executor;

public class SingleHttpSource extends RichHttpSourceFunction<Map<String, byte[]>> {

    private static final Logger logger = LoggerFactory.getLogger(SingleHttpSource.class);

    private static final String KNOWLEDGE_EXPR = "$.[?(@.version=='latest' && @.name in ['ip_v4','asn_v4','asn_v6','websktDB_full-20211123-101740','webskt_default_dns_server_info_full_20220427','icp','link_direction','xjlt_ip_location_20220422','xjlt_idc_renter_20220422','xjlt_idc_internal_ip_20220422','app_sketch_labeled','XJ_top'])].['name','sha256','format','path']";


    private Properties nacosProperties;

    private String NACOS_DATA_ID;

    private String NACOS_GROUP;

    private long NACOS_READ_TIMEOUT;


    private ConfigService configService;

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(List.class, KnowledgeLog.class);
    private static Map<String, String> updateMap = new HashMap<>();
    private static HashMap<String, byte[]> knowledgeFileCache;

    private boolean isRunning = true;


    public SingleHttpSource(Properties nacosProperties, String NACOS_DATA_ID, String NACOS_GROUP, long NACOS_READ_TIMEOUT) {
        this.nacosProperties = nacosProperties;
        this.NACOS_DATA_ID = NACOS_DATA_ID;
        this.NACOS_GROUP = NACOS_GROUP;
        this.NACOS_READ_TIMEOUT = NACOS_READ_TIMEOUT;
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
        if (StringUtil.isNotBlank(config)) {
            ArrayList<Object> metaList = JsonPath.parse(config).read(KNOWLEDGE_EXPR);
            loadKnowledge(metaList);
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
            FileUtil.mkdir(CommonConfig.DOWNLOAD_PATH);
            File file = new File(CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(fileName));
            outputStream = new FileOutputStream(file);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            knowledgeFileCache.put(fileName, bytes);
            inputStream=new ByteArrayInputStream(bytes);
            IoUtil.copy(inputStream, outputStream);
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

