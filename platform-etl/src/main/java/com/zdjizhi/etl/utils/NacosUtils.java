package com.zdjizhi.etl.utils;

import cn.hutool.core.io.IoUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.KnowledgeConstant;
import com.zdjizhi.base.common.KnowledgeLog;
import com.zdjizhi.base.utils.CommonConfigurations;
import com.zdjizhi.base.utils.HttpClientUtils;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executor;


public class NacosUtils {


    private static final Logger logger = LoggerFactory.getLogger(NacosUtils.class);
    private static CommonConfigurations configurations;

    static {
        Properties propService;
        try {
            propService = new Properties();
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("common.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载common.properties配置文件失败");
            System.exit(1);
        }
    }



    private static Properties nacosProperties = new Properties();
    private static Properties commonProperties = new Properties();

    private static Map<String, String> updateMap = new HashMap<>();

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(List.class, KnowledgeLog.class);

    private static final String NACOS_SERVER_ADDR = configurations.getStringProperty("nacos.server.addr");
    private static final String NACOS_NAMESPACE = configurations.getStringProperty("nacos.namespace");
    private static final String NACOS_USERNAME = configurations.getStringProperty("nacos.username");
    private static final String NACOS_PASSWORD = configurations.getStringProperty("nacos.password");
    private static final String NACOS_DATA_ID = configurations.getStringProperty("nacos.data.id");
    private static final String NACOS_GROUP = configurations.getStringProperty("nacos.group");
    private static final long NACOS_READ_TIMEOUT = configurations.getLongProperty("nacos.read.timeout");

    static {
        createConfigService();
    }

    private static void getProperties() {
        nacosProperties.setProperty(PropertyKeyConst.SERVER_ADDR, NACOS_SERVER_ADDR);
        nacosProperties.setProperty(PropertyKeyConst.NAMESPACE, NACOS_NAMESPACE);
        nacosProperties.setProperty(PropertyKeyConst.USERNAME, NACOS_USERNAME);
        nacosProperties.setProperty(PropertyKeyConst.PASSWORD, NACOS_PASSWORD);
    }

    public static void createConfigService() {
        try {
            getProperties();
            ConfigService configService = NacosFactory.createConfigService(nacosProperties);
            String config = configService.getConfig(NACOS_DATA_ID, NACOS_GROUP, NACOS_READ_TIMEOUT);

            loadKnowledge(config);

            configService.addListener(NACOS_DATA_ID, NACOS_GROUP, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String configMsg) {
                    try {
                        loadKnowledge(configMsg);
                    } catch (Exception e) {
                        logger.error("监听nacos配置失败", e);
                    }
                    System.out.println(configMsg);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("获取nacos配置失败", e);
        }

    }


    //加载nacos配置
    public static void loadKnowledge(String config){
        Map<String, KnowledgeLog> knowledgeLogMap = queryLoadKnowledgeInfo(config);


        for (Map.Entry<String, KnowledgeLog> knowledgeLogEntry : knowledgeLogMap.entrySet()) {
            KnowledgeLog knowledgeLog = knowledgeLogEntry.getValue();
            String id = knowledgeLog.getId();
            String sha256 = knowledgeLog.getSha256();
            if (!StringUtil.equals(sha256,updateMap.get(id))){
                System.out.println(knowledgeLogEntry.getValue());
                download( knowledgeLogEntry.getValue());
            }
        }
        Map<String, String> path = queryPath(knowledgeLogMap);
        IPUtils.initIpLookup(path.get(KnowledgeConstant.IP_V4),path.get(KnowledgeConstant.ASN_V4),path.get(KnowledgeConstant.ASN_V6));
        IdcRenterUtils.readServerCsv(path.get(KnowledgeConstant.ISP));
        LinkUtils.readLinkCsv(path.get(KnowledgeConstant.LINK));
        IspUtils.readServerCsv(path.get(KnowledgeConstant.ISP));
        FcUtils.readCsv(path.get(KnowledgeConstant.WEBSKT));
        FcUtils.readIcpCsv(path.get(KnowledgeConstant.ICP));
        WhoisUtils.readJson(path.get(KnowledgeConstant.WHOIS));
        DnsServerUtils.readServerCsv(path.get(KnowledgeConstant.DNSPATH));
        AppUtils.readJson(path.get(KnowledgeConstant.APPSKT));
        IPUtils.readInternalIpCsv(path.get(KnowledgeConstant.INTERNALIP));

    }

    //query知识库中json信息
    public static Map<String,KnowledgeLog> queryLoadKnowledgeInfo(String content){
        Map<String, KnowledgeLog> map = new HashMap<>();

        List<KnowledgeLog> o = jsonMapperInstance.fromJson(content, listType);
        for (KnowledgeLog knowledgeLog : o) {
                if ((KnowledgeConstant.IP_V4.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))) && (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.IP_V4,knowledgeLog);
                }
                if (((KnowledgeConstant.ASN_V4.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))) )&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.ASN_V4,knowledgeLog);
                }
                if ((KnowledgeConstant.ASN_V6.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion())) ){
                    map.put(KnowledgeConstant.ASN_V6,knowledgeLog);
                }
                if ((KnowledgeConstant.WEBSKT.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion())) )){
                    map.put(KnowledgeConstant.WEBSKT,knowledgeLog);
                }
                if ((KnowledgeConstant.DNSPATH.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.DNSPATH,knowledgeLog);
                }
                if ((KnowledgeConstant.APPSKT.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.APPSKT,knowledgeLog);
                }
                if ((KnowledgeConstant.WHOIS.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.WHOIS,knowledgeLog);
                }
                if( (KnowledgeConstant.ICP.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.ICP,knowledgeLog);
                }
                if ((KnowledgeConstant.IOC.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.IOC,knowledgeLog);
                }
                if ((KnowledgeConstant.LINK.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.LINK,knowledgeLog);
                }
                if (KnowledgeConstant.ISP.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.ISP,knowledgeLog);
                }
                if (KnowledgeConstant.IDCRENTER.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.IDCRENTER,knowledgeLog);
                }
                if (KnowledgeConstant.INTERNALIP.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& (KnowledgeConstant.LATEST.equals(knowledgeLog.getVersion()))){
                    map.put(KnowledgeConstant.INTERNALIP,knowledgeLog);
                }
        }
        return map;
    }





    //匹配路径
    private static Map<String,String> queryPath(Map<String,KnowledgeLog> map){
        Map<String,String> pathMap=new HashMap<>();
        KnowledgeLog ip_v4 = map.get(KnowledgeConstant.IP_V4);
        KnowledgeLog asn_v4 = map.get(KnowledgeConstant.ASN_V4);
        KnowledgeLog asn_v6 = map.get(KnowledgeConstant.ASN_V6);
        KnowledgeLog webskt = map.get(KnowledgeConstant.WEBSKT);
        KnowledgeLog dnsPath = map.get(KnowledgeConstant.DNSPATH);
        KnowledgeLog appskt = map.get(KnowledgeConstant.APPSKT);
        KnowledgeLog whois = map.get(KnowledgeConstant.WHOIS);
        KnowledgeLog icp = map.get(KnowledgeConstant.ICP);
        KnowledgeLog ioc = map.get(KnowledgeConstant.IOC);
        KnowledgeLog link = map.get(KnowledgeConstant.LINK);
        KnowledgeLog isp = map.get(KnowledgeConstant.ISP);
        KnowledgeLog idcRenter = map.get(KnowledgeConstant.IDCRENTER);
        KnowledgeLog internalIp = map.get(KnowledgeConstant.INTERNALIP);

        if (StringUtil.isEmpty(ip_v4)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(ip_v4.getName().concat(".").concat(ip_v4.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.IP_V4,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.IP_V4);
        }else{
            pathMap.put(KnowledgeConstant.IP_V4,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(ip_v4.getName()).concat(".").concat(ip_v4.getFormat()));
        }
        if (StringUtil.isEmpty(asn_v4)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(asn_v4.getName().concat(".").concat(asn_v4.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.ASN_V4,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.ASN_V4);
        }else{
            pathMap.put(KnowledgeConstant.ASN_V4,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(asn_v4.getName()).concat(".").concat(asn_v4.getFormat()));
        }
        if (StringUtil.isEmpty(asn_v6)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(asn_v6.getName().concat(".").concat(asn_v6.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.ASN_V6,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.ASN_V6);
        }else{
            pathMap.put(KnowledgeConstant.ASN_V6,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(asn_v6.getName()).concat(".").concat(asn_v6.getFormat()));
        }
        if (StringUtil.isEmpty(webskt)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(webskt.getName().concat(".").concat(webskt.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.WEBSKT,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.WEBSKT);
        }else{
            pathMap.put(KnowledgeConstant.WEBSKT,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(webskt.getName()).concat(".").concat(webskt.getFormat()));
        }
        if (StringUtil.isEmpty(dnsPath)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(dnsPath.getName().concat(".").concat(dnsPath.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.DNSPATH,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.DNSPATH);
        }else{
            pathMap.put(KnowledgeConstant.DNSPATH,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(dnsPath.getName()).concat(".").concat(dnsPath.getFormat()));
        }
        if (StringUtil.isEmpty(appskt)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(appskt.getName().concat(".").concat(appskt.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.APPSKT,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.APPSKT);
        }else{
            pathMap.put(KnowledgeConstant.APPSKT,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(appskt.getName()).concat(".").concat(appskt.getFormat()));
        }
        if (StringUtil.isEmpty(whois)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(whois.getName().concat(".").concat(whois.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.WHOIS,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.WHOIS);
        }else{
            pathMap.put(KnowledgeConstant.WHOIS,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(whois.getName()).concat(".").concat(whois.getFormat()));
        }
        if (StringUtil.isEmpty(icp)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(icp.getName().concat(".").concat(icp.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.ICP,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.ICP);
        }else{
            pathMap.put(KnowledgeConstant.ICP,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(icp.getName()).concat(".").concat(icp.getFormat()));
        }
        if (StringUtil.isEmpty(ioc)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(ioc.getName().concat(".").concat(ioc.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.IOC,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.IOC);
        }else{
            pathMap.put(KnowledgeConstant.IOC,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(ioc.getName()).concat(".").concat(ioc.getFormat()));
        }
        if (StringUtil.isEmpty(link)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(link.getName().concat(".").concat(link.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.LINK,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.LINK);
        }else{
            pathMap.put(KnowledgeConstant.LINK,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(link.getName()).concat(".").concat(link.getFormat()));
        }
        if (StringUtil.isEmpty(isp)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(isp.getName().concat(".").concat(isp.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.ISP,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.ISP);
        }else{
            pathMap.put(KnowledgeConstant.ISP,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(isp.getName()).concat(".").concat(isp.getFormat()));
        }
        if (StringUtil.isEmpty(idcRenter)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(idcRenter.getName().concat(".").concat(idcRenter.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.IDCRENTER,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.IDCRENTER);
        }else{
            pathMap.put(KnowledgeConstant.IDCRENTER,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(idcRenter.getName()).concat(".").concat(idcRenter.getFormat()));
        }
        if (StringUtil.isEmpty(internalIp)|| !new File(CommonConfig.DOWNLOAD_PATH.concat(internalIp.getName().concat(".").concat(internalIp.getFormat()))).exists()){
            pathMap.put(KnowledgeConstant.INTERNALIP,CommonConfig.DOWNLOAD_PATH+File.separator+KnowledgeConstant.INTERNALIP);
        }else{
            pathMap.put(KnowledgeConstant.INTERNALIP,CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(internalIp.getName()).concat(".").concat(internalIp.getFormat()));
        }
        return pathMap;
    }


    //下载文件
    public static void download(KnowledgeLog knowledgeLog){
        String id = knowledgeLog.getId();
        String name = knowledgeLog.getName();
        String sha256 = knowledgeLog.getSha256();
        String format = knowledgeLog.getFormat();

        InputStream inputStream = null;
        FileOutputStream outputStream = null;

        try {
            Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            inputStream = httpClientUtils.httpGetInputStream(knowledgeLog.getPath(), 3000, header);
            File file=new File(CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(knowledgeLog.getName()).concat(".").concat(knowledgeLog.getFormat()));
            outputStream=new FileOutputStream(file);
            IoUtil.copy(inputStream, outputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            IoUtil.close(inputStream);
            IoUtil.close(outputStream);
        }
        updateMap.put(id,sha256);
    }

    public static String getStringProperty(String key) {
        return commonProperties.getProperty(key);
    }

    public static Integer getIntProperty(String key) {
        return Integer.parseInt(commonProperties.getProperty(key));
    }

    public static Double getDoubleProperty(String key) {
        return Double.parseDouble(commonProperties.getProperty(key));
    }

    public static Long getLongProperty(String key) {
        return Long.parseLong(commonProperties.getProperty(key));
    }

    public static Boolean getBooleanProperty(String key) {
        return "true".equals(commonProperties.getProperty(key).toLowerCase().trim());
    }
}
