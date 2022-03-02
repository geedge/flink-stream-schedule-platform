package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
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

    private static HashMap<String, CnRecordLog> smap = readJson();

    private static String readJsonFile(String fileName) {
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
            LOG.error(e.toString());
            return null;
        }
    }

    private static HashMap<String, CnRecordLog> readJson() {
        HashMap<String, CnRecordLog> appmap = new HashMap<>();
        try {
            String s = readJsonFile(CommonConfig.APPSKT_PATH);
            ArrayList<HashMap<String, Object>>  jarray = jsonMapperInstance.fromJson(s, listType);
            LOG.info(""+jarray.size());
            for (HashMap<String, Object> obj : jarray) {
                try {
                    CnRecordLog richEntity = new CnRecordLog();
                    HashMap<String, Object> properties = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(obj.get("appProperties")), hashmapJsonType);
                    richEntity.setApp_category(properties.get("category").toString());
                    String risk = properties.get("risk").toString();
                    if (StringUtil.isNotEmpty(risk)){
                        richEntity.setApp_risk(Long.parseLong(risk));
                    }
                    richEntity.setApp_subcategory(properties.get("subcategory").toString());
                    appmap.put(obj.get("appName").toString(), richEntity);
                }catch (Exception e){
                    LOG.error("解析app异常:{}",obj);
                }
            }
        } catch (Exception e) {
            LOG.error("transaction app json error "+e.toString());
        }
        return appmap;
    }

    public static void getAppSketchMessage(CnRecordLog entity) {
        try {
            if (smap.containsKey(entity.getCommon_app_label())) {
                entity.setApp_subcategory(smap.get(entity.getCommon_app_label()).getApp_subcategory());
                entity.setApp_risk(smap.get(entity.getCommon_app_label()).getApp_risk());
                entity.setApp_category(smap.get(entity.getCommon_app_label()).getApp_category());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void main(String[] args) throws Exception {

        HashMap<String, CnRecordLog> map = readJson();
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}