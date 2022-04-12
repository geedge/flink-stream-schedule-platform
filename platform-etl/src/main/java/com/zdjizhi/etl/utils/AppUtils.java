package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.AppSketchLabel;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class AppUtils {
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    private static HashMap<String, AppSketchLabel> smap = readJson();

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
            LOG.error("解析{} 文件失败,{}",fileName,e);
            return null;
        }
    }

    private static HashMap<String, AppSketchLabel> readJson() {
        HashMap<String, AppSketchLabel> appmap = new HashMap<>();
        try {
            String s = readJsonFile(CommonConfig.APPSKT_PATH);
            ArrayList<HashMap<String, Object>>  jarray = jsonMapperInstance.fromJson(s, listType);
            LOG.info(""+jarray.size());
            for (HashMap<String, Object> obj : jarray) {
                try {
                    AppSketchLabel appSketchLabel = new AppSketchLabel();
                    HashMap<String, Object> properties = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(obj.get("appProperties")), hashmapJsonType);
                    appSketchLabel.setCategory(properties.get("category").toString());
                    String risk = properties.get("risk").toString();
                    if (StringUtil.isNotEmpty(risk)){
                        appSketchLabel.setRisk(Long.parseLong(risk));
                    }
                    appSketchLabel.setSubCategory(properties.get("subcategory").toString());
                    appSketchLabel.setProtocol(Integer.parseInt(properties.getOrDefault("isProtocol","0").toString()));
                    appSketchLabel.setAppCompany(properties.getOrDefault("appCompany","").toString());
                    appmap.put(obj.get("appName").toString(), appSketchLabel);
                }catch (Exception e){
                    LOG.error("解析app异常:{}\n{}",obj,e);
                }
            }
        } catch (Exception e) {
            LOG.error("transaction app json error :{}",e);
        }
        return appmap;
    }

    public static void getAppSketchMessage(CnRecordLog entity) {
        String commonAppLabel = entity.getCommon_app_label();
        if (smap.containsKey(commonAppLabel)) {
            AppSketchLabel appSketchLabel = smap.get(commonAppLabel);
            entity.setApp_subcategory(appSketchLabel.getSubCategory());
            entity.setApp_risk(appSketchLabel.getRisk());
            entity.setApp_category(appSketchLabel.getCategory());
            entity.setApp_is_protocol(appSketchLabel.getProtocol());
            entity.setApp_company(appSketchLabel.getAppCompany());
        }
    }

    public static void main(String[] args) throws Exception {

        HashMap<String, AppSketchLabel> map = readJson();
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}