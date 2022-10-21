package com.zdjizhi.base.utils;

import cn.hutool.core.io.IoUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.KnowledgeConstant;
import com.zdjizhi.base.common.KnowledgeLog;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.table.runtime.util.JsonUtils;
import org.apache.http.Header;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicHeader;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executor;

public class NacosTest {

    /**
     * <dependency>
     * <groupId>com.alibaba.nacos</groupId>
     * <artifactId>nacos-client</artifactId>
     * <version>1.2.0</version>
     * </dependency>
     */

    private static Properties properties = new Properties();
    /**
     * config data id = config name
     */
    private static final String DATA_ID = "knowledge_base.json";
    /**
     * config group
     */
    private static final String GROUP = "DEFAULT_GROUP";


    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(List.class, KnowledgeLog.class);
    private static Map<String, String> updateMap = new HashMap<>();
//    private static HashMap<String, DomainWhois> smap = readJson();


    private void getProperties() {
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, "192.168.44.12:8848");
        properties.setProperty(PropertyKeyConst.NAMESPACE, "public");
        properties.setProperty(PropertyKeyConst.USERNAME, "nacos");
        properties.setProperty(PropertyKeyConst.PASSWORD, "nacos");
    }


    @Test
    public void GetConfigurationTest() {
        InputStream inputStream = null;
        FileOutputStream outputStream = null;
        try {
            getProperties();
            ConfigService configService = NacosFactory.createConfigService(properties);
            String content = configService.getConfig(DATA_ID, GROUP, 5000);
            Properties nacosConfigMap = new Properties();
//            KnowledgeLog knowledgeLog = new KnowledgeLog();
//            HttpClientUtils httpClientUtils = new HttpClientUtils();



            Map<String, KnowledgeLog> knowledgeLogMap = queryLoadKnowledgeInfo(content);


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
            readJson(path.get(KnowledgeConstant.WHOIS));
//            checkFile(content);



//            HashMap<String, DomainWhois> map = readJson(pathMap);
//            Set<String> keySet = map.keySet();
//            for (String key : keySet){
//                System.out.println(map.get(key).toString());
//            }
//            System.out.println(map.size());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void readJson(String path) {
        HashMap<String, DomainWhois> whoismap = new HashMap<>();
        try {

            String s = readJsonFile(path);
            ArrayList<HashMap<String, Object>>  jarray = jsonMapperInstance.fromJson(s, listType);
            for (HashMap<String, Object> obj : jarray) {
                DomainWhois richEntity = new DomainWhois();
                richEntity.setFqdn(obj.get("fqdn").toString());
                if (obj.containsKey("whois_registrant_org")){
                    richEntity.setWhoisRegistrantOrg(obj.get("whois_registrant_org").toString());
                }
                whoismap.put(richEntity.getFqdn(), richEntity);
            }
        } catch (Exception e) {

        }
    }

    static class DomainWhois{
        private String fqdn;
        private String whoisRegistrantOrg;

        public String getFqdn() {
            return fqdn;
        }

        public void setFqdn(String fqdn) {
            this.fqdn = fqdn;
        }

        public String getWhoisRegistrantOrg() {
            return whoisRegistrantOrg;
        }

        public void setWhoisRegistrantOrg(String whoisRegistrantOrg) {
            this.whoisRegistrantOrg = whoisRegistrantOrg;
        }

        @Override
        public String toString() {
            return "DomainWhois{" +
                    "fqdn='" + fqdn + '\'' +
                    ", whoisRegistrantOrg='" + whoisRegistrantOrg + '\'' +
                    '}';
        }
    }
    //query知识库中json信息
    public static Map<String,KnowledgeLog> queryLoadKnowledgeInfo(String content){
        Map<String, KnowledgeLog> map = new HashMap<>();

        List<KnowledgeLog> o = jsonMapperInstance.fromJson(content, listType);
        for (KnowledgeLog knowledgeLog : o) {

//            Map<String, String> downloadPathMap = download(knowledgeLog);
//            for (Map.Entry<String, String> path : downloadPathMap.entrySet()) {
//                String pathValue = path.getValue();
            if (KnowledgeConstant.WEBSKT.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat())) && StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.WEBSKT,knowledgeLog);
            }
            if (KnowledgeConstant.DNSPATH.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.DNSPATH,knowledgeLog);
            }
            if (KnowledgeConstant.APPSKT.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.APPSKT,knowledgeLog);
            }
            if (KnowledgeConstant.WHOIS.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.WHOIS,knowledgeLog);
            }
            if (KnowledgeConstant.ICP.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.ICP,knowledgeLog);
            }
            if (KnowledgeConstant.IOC.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.IOC,knowledgeLog);
            }
            if (KnowledgeConstant.LINK.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.LINK,knowledgeLog);
            }
            if (KnowledgeConstant.ISP.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.ISP,knowledgeLog);
            }
            if (KnowledgeConstant.IDCRENTER.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.IDCRENTER,knowledgeLog);
            }
            if (KnowledgeConstant.INTERNALIP.equals(knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat()))&& StringUtil.equals(knowledgeLog.getVersion(), KnowledgeConstant.LATEST)){
                map.put(KnowledgeConstant.INTERNALIP,knowledgeLog);
            }
//            }

            download(knowledgeLog);
        }
        return map;
    }


    //下载文件
    public static void download(KnowledgeLog knowledgeLog){
        String id = knowledgeLog.getId();
        String name = knowledgeLog.getName();
        String sha256 = knowledgeLog.getSha256();
        String format = knowledgeLog.getFormat();
//        String path=null;
//        Map<String,String> map=new HashMap<>();
        Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
        InputStream inputStream = null;
        FileOutputStream outputStream = null;
        HttpClientUtils httpClientUtils = new HttpClientUtils();
        try {
            inputStream = httpClientUtils.httpGetInputStream(knowledgeLog.getPath(), 3000, header);
            File file=new File(CommonConfig.DOWNLOAD_PATH.concat(File.separator).concat(knowledgeLog.getName()).concat(".").concat(knowledgeLog.getFormat()));

//            String name=knowledgeLog.getName().concat(".").concat(knowledgeLog.getFormat());
//            map.put(name,file.toString());

            outputStream=new FileOutputStream(file);
            IoUtil.copy(inputStream, outputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            IoUtil.close(inputStream);
            IoUtil.close(outputStream);
        }
//        return path;
//        return map;
        updateMap.put(id,sha256);
    }


    //匹配路径
    private static Map<String,String> queryPath(Map<String,KnowledgeLog> map){
        Map<String,String> pathMap=new HashMap<>();
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


    public static String readJsonFile(String fileName) {
        String jsonStr;
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (Exception e) {

            return null;
        }
    }


    @Test
    public void ListenerConfigurationTest() {
        getProperties();
        try {
            //first get config
            ConfigService configService = NacosFactory.createConfigService(properties);
            String config = configService.getConfig(DATA_ID, GROUP, 5000);
//            System.out.println(config);

            //start listenner
            configService.addListener(DATA_ID, GROUP, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String configMsg) {
                    System.out.println(configMsg);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        //keep running,change nacos config,print new config
        /*
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        */
    }
}
