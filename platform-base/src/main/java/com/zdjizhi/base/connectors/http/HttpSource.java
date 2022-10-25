package com.zdjizhi.base.connectors.http;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.common.KnowledgeLog;
import com.zdjizhi.base.function.source.RichHttpSourceFunction;
import com.zdjizhi.base.utils.CommonConfigurations;
import com.zdjizhi.base.utils.HttpClientUtils;
import com.zdjizhi.utils.JsonMapper;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

public class HttpSource extends RichHttpSourceFunction<List<CustomFile>> {

    private static final Logger logger = LoggerFactory.getLogger(HttpSource.class);

    private Properties nacosProperties;

    private String NACOS_DATA_ID;

    private String NACOS_GROUP;

    private long NACOS_READ_TIMEOUT;

    private ConfigService configService;

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(List.class, KnowledgeLog.class);

    private boolean isRunning = true;


    public HttpSource(Properties nacosProperties, String NACOS_DATA_ID, String NACOS_GROUP, long NACOS_READ_TIMEOUT) {
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
        String config = configService.getConfig(NACOS_DATA_ID, NACOS_GROUP, NACOS_READ_TIMEOUT);

        List<CustomFile> customFiles = new ArrayList<>();
        customFiles = loadKnowledge(config);
        ctx.collectWithTimestamp(customFiles,System.currentTimeMillis());

        configService.addListener(NACOS_DATA_ID, NACOS_GROUP, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configMsg) {
                try {
                    List<CustomFile> customFiles = new ArrayList<>();
                    customFiles = loadKnowledge(configMsg);
                    ctx.collectWithTimestamp(customFiles,System.currentTimeMillis());
                } catch (Exception e) {
                    logger.error("监听nacos配置失败", e);
                }
                System.out.println(configMsg);
            }
        });



    }

    public List<CustomFile> loadKnowledge(String config) {
        List<CustomFile> customFiles = new ArrayList<>();
        List<KnowledgeLog> knowledges = jsonMapperInstance.fromJson(config, listType);
        for (KnowledgeLog knowledge : knowledges) {
            CustomFile customFile = new CustomFile();
            String fileName = knowledge.getName().concat(".").concat(knowledge.getFormat());
            customFile.setFileName(fileName);
            InputStream inputStream = null;
            try {
                Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
                HttpClientUtils httpClientUtils = new HttpClientUtils();
                inputStream = httpClientUtils.httpGetInputStream(knowledge.getPath(), 3000, header);
                byte[] bytes = IOUtils.toByteArray(inputStream);
                customFile.setContent(bytes);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
            customFiles.add(customFile);
        }
        return customFiles;
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
