package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.AppSketchLabel;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class AppUtils {
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    private static HashMap<String, AppSketchLabel> smap = readJson();

    private static HashMap<String, AppSketchLabel> readJson() {
        HashMap<String, AppSketchLabel> appmap = new HashMap<>();
        try {
            String s = JsonUtils.readJsonFile(CommonConfig.APPSKT_PATH);
            ArrayList<HashMap<String, Object>>  jarray = jsonMapperInstance.fromJson(s, listType);
            LOG.info(""+jarray.size());
            for (HashMap<String, Object> obj : jarray) {
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
                    String appName = obj.get("appName").toString();
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
        return appmap;
    }

    public static void setAppSketchMessage(CnRecordLog entity) {
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