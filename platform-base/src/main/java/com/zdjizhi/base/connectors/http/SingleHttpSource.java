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
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.common.KnowledgeConstant;
import com.zdjizhi.base.common.KnowledgeLog;
import com.zdjizhi.base.function.source.RichHttpSourceFunction;
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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

public class SingleHttpSource extends RichHttpSourceFunction<List<CustomFile>> {

    private static final Logger logger = LoggerFactory.getLogger(HttpSource.class);

    private Properties nacosProperties;

    private String NACOS_DATA_ID;

    private String NACOS_GROUP;

    private long NACOS_READ_TIMEOUT;

    private static String STORE_PATH;

    private ConfigService configService;

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(List.class, KnowledgeLog.class);
    private static Map<String, String> updateMap = new HashMap<>();

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
        logger.info("连接nacos：" + nacosProperties.getProperty(PropertyKeyConst.SERVER_ADDR));
        configService = NacosFactory.createConfigService(nacosProperties);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
        String config = configService.getConfig(NACOS_DATA_ID, NACOS_GROUP, NACOS_READ_TIMEOUT);
        List<CustomFile> customFiles = new ArrayList<>();

        if (StringUtil.isNotBlank(config)) {
            List<KnowledgeLog> knowledgeLogListList = jsonMapperInstance.fromJson(config, listType);
            if (knowledgeLogListList.size()>=1){
                for (KnowledgeLog knowledgeLog : knowledgeLogListList) {
                    String name = knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat());
                    String sha256 = knowledgeLog.getSha256();
                    updateMap.put(name,sha256);
                }
            }
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
                    ArrayList<CustomFile> customFiles = new ArrayList<>();
                    List<KnowledgeLog> knowledgeInfo = jsonMapperInstance.fromJson(configMsg, listType);
                    for (KnowledgeLog knowledgeLog : knowledgeInfo) {
                        String fileName = knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat());
                        String sha256 = knowledgeLog.getSha256();
                        String filePath = knowledgeLog.getPath();
                        if (updateMap.containsKey(fileName)){
                            if (!sha256.equals(updateMap.get(fileName))){
                                CustomFile customFile = loadKnowledge(fileName, filePath);
                                customFiles.add(customFile);
                                updateMap.put(fileName,sha256);
                            }
                        }else {
                            updateMap.put(fileName, sha256);
                            CustomFile customFile = loadKnowledge(fileName, filePath);
                            customFiles.add(customFile);
                        }
                    }
//                    customFiles=loadKnowledge(configMsg);
                    ctx.collectWithTimestamp(customFiles,System.currentTimeMillis());

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

    private CustomFile loadKnowledge(String fileName, String filePath) {
        InputStream inputStream = null;
        FileOutputStream outputStream = null;
        CustomFile customFile = new CustomFile();
        try {
            customFile.setFileName(fileName);
            Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            inputStream = httpClientUtils.httpGetInputStream(filePath, 3000, header);
            FileUtil.mkdir(CommonConfig.DOWNLOAD_PATH);
            File file = new File(CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(fileName));
            outputStream = new FileOutputStream(file);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            customFile.setContent(bytes);
            inputStream = new ByteArrayInputStream(customFile.getContent());
            IoUtil.copy(inputStream, outputStream);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
        return customFile;
    }


    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

