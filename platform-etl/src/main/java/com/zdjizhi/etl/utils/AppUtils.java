package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.AppSketchLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class AppUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AppUtils.class);

    private static HashMap<String, AppSketchLabel> appMap = new HashMap<>();

    public static void readCsv(HighCsvReader csvReader) {
        HashMap<String, AppSketchLabel> newAppMap = new HashMap<>(csvReader.getLineNumber() * 2);
        HighCsvReader.CsvIterator iterator = csvReader.getIterator();
        while (iterator.hasNext()) {
            Map<String, String> line = iterator.next();
            try {
                String appName = line.get("app_name");
                String category = line.get("app_category");
                String subCategory = line.get("app_subcategory");
                String riskStr = line.get("app_risk");
                long risk = Long.parseLong("".equals(riskStr) ? "0" : riskStr);
                String protocolStr = line.get("app_is_protocol");
                int protocol = Integer.parseInt("".equals(protocolStr) ? "0" : protocolStr);
                String appCompany = line.get("app_company");

                AppSketchLabel appSketchLabel = new AppSketchLabel();
                appSketchLabel.setAppName(appName);
                appSketchLabel.setCategory(category);
                appSketchLabel.setSubCategory(subCategory);
                appSketchLabel.setRisk(risk);
                appSketchLabel.setProtocol(protocol);
                if (appSketchLabel.getProtocol() != 1) {
                    appSketchLabel.setAppCompany(appCompany);
                } else {
                    appSketchLabel.setAppCompany(appName);
                }

                newAppMap.put(appSketchLabel.getAppName(), appSketchLabel);
            } catch (Exception lineException) {
                System.out.println("AppUtils line: " + line.toString() + " parse error：" + lineException);
                lineException.printStackTrace(System.out);
            }
        }
        appMap = newAppMap;
    }

    public static void setAppSketchMessage(CnRecordLog entity) {
        try {
            setAppLabel(entity);
            String appName = entity.getCommon_app_label();
            if (!"unknown".equals(appName)){
                if (appMap.containsKey(appName)) {
                    AppSketchLabel appSketchLabel = appMap.get(appName);
                    entity.setApp_subcategory(appSketchLabel.getSubCategory());
                    entity.setApp_risk(appSketchLabel.getRisk());
                    entity.setApp_category(appSketchLabel.getCategory());
                    entity.setApp_is_protocol(appSketchLabel.getProtocol());
                    entity.setApp_company(appSketchLabel.getAppCompany());
                }
            }else {
                entity.setCommon_app_label(null);
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void setAppLabel(CnRecordLog entity){
        Object commonAppId = entity.getCommon_app_id();
        String commonAppLabel = entity.getCommon_app_label();
        if (CommonConfig.APP_LABEL_IS_THIRD == 0){
            if (commonAppId != null){
                String appIdJsonString = JSON.toJSONString(commonAppId);
                entity.setCommon_app_id(appIdJsonString);
                HashMap<String, Object> appId = JSON.parseObject(appIdJsonString, TypeReference.mapType(HashMap.class, String.class, Object.class));
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
}