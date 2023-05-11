package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.AppSketchLabel;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;

public class AppUtils {
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    private static HashMap<String, AppSketchLabel> appmap = new HashMap<>();
    private static HashMap<Integer, String> appIdMap = new HashMap<>();


    public static void readJson(BufferedReader appsktBufferedReader) {
        try {
            String appsktInput = "";
            StringBuilder appBuilder = new StringBuilder();
            while ((appsktInput = appsktBufferedReader.readLine()) != null) {
                appBuilder.append(appsktInput);
            }
            ArrayList<HashMap<String, Object>> app = jsonMapperInstance.fromJson(appBuilder.toString(), listType);
//            LOG.info(""+app.size());
            for (HashMap<String, Object> obj : app) {
                try {
                    AppSketchLabel appSketchLabel = new AppSketchLabel();
                    HashMap<String, Object> properties = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(obj.get("appProperties")), hashmapJsonType);
                    appSketchLabel.setCategory(properties.getOrDefault("category","").toString());
                    String risk = properties.getOrDefault("risk","").toString();
                    if (StringUtil.isNotEmpty(risk)){
                        appSketchLabel.setRisk(Long.parseLong(risk));
                    }
                    appSketchLabel.setSubCategory(properties.getOrDefault("subcategory","").toString());
                    int isProtocol = Integer.parseInt(properties.getOrDefault("isProtocol", "0").toString());
                    appSketchLabel.setProtocol(isProtocol);
                    String appCompany = properties.getOrDefault("appCompany", "").toString();
                    Integer appId = (Integer)obj.get("appId");
                    String appName = obj.get("appName").toString();
                    appIdMap.put(appId,appName);
                    if (isProtocol != 1){
                        if (StringUtil.isNotBlank(appCompany)){
                            appSketchLabel.setAppCompany(appCompany);
                        }else {
                            appSketchLabel.setAppCompany(appName);
                        }
                    }
                    appmap.put(appName, appSketchLabel);
                }catch (Exception e){
                    LOG.error("解析app异常:{}\n{}",obj,e);
                }
            }
        } catch (Exception e) {
            LOG.error("transaction app json error :{}",e);
        }
    }

    public static void setAppSketchMessage(CnRecordLog entity) {
        try {
            setAppLabel(entity);
            String appName = entity.getCommon_app_label();
            if (!"unknown".equals(appName)){
                if (appmap.containsKey(appName)) {
                    AppSketchLabel appSketchLabel = appmap.get(appName);
                    entity.setApp_subcategory(appSketchLabel.getSubCategory());
                    entity.setApp_risk(appSketchLabel.getRisk());
                    entity.setApp_category(appSketchLabel.getCategory());
                    entity.setApp_is_protocol(appSketchLabel.getProtocol());
                    entity.setApp_company(appSketchLabel.getAppCompany());
                }
            }else {
                entity.setCommon_app_label(null);
                AppSketchLabel appSketchLabel = appmap.get(appName);
                appSketchLabel.getAppCompany();
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static void setAppLabel(CnRecordLog entity){
        Object commonAppId = entity.getCommon_app_id();
        String commonAppLabel = entity.getCommon_app_label();
        if (CommonConfig.APP_LABEL_IS_THIRD == 0){
            if (commonAppId != null){
                String appIdJsonString = jsonMapperInstance.toJson(commonAppId);
                entity.setCommon_app_id(appIdJsonString);
                HashMap<String, Object> appId = jsonMapperInstance.fromJson(appIdJsonString, hashmapJsonType);
                ArrayList<HashMap<String, Object>> third = (ArrayList<HashMap<String, Object>>)appId.getOrDefault("THIRD", null);
                if (third != null){
                    String appName = (String) third.get(third.size() - 1).getOrDefault("app_name", "");
                    entity.setCommon_app_label(appName);
                }else {
                    entity.setCommon_app_label("");
                }
            }else {
                entity.setCommon_app_label("");
            }
        }else {
            entity.setCommon_app_label(commonAppLabel);
        }
    }

//    public static void main(String[] args) throws Exception {
//        /*
//        for (String key : appmap.keySet()){
//            System.out.println(appmap.get(key).toString());
//        }
//        */
//        System.out.println(appmap.size());
//
//        /*
//        for (Integer id: appIdMap.keySet()){
//            String x = appIdMap.get(id);
//            System.out.println(id+"----"+x);
//        }
//        */
//        System.out.println(appIdMap.size());
//
//    }
}