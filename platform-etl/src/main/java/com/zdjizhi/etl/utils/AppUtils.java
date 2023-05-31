package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FileByteUtils;
import com.zdjizhi.base.utils.HdfsUtils;
import com.zdjizhi.etl.common.AppSketchLabel;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class AppUtils {
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);

    private static HashMap<String, AppSketchLabel> appmap = new HashMap<>();

    public static void readCsv(Reader appsktReader) {
        try {
            CSVReader csvReader = new CSVReader(appsktReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<String, AppSketchLabel> newAppmap = new HashMap<>(lines.size() * 2);
            for (String[] line : lines) {
                try {
                    String appName = line[1];
                    String category = line[3];
                    String subCategory = line[4];
                    long risk = Long.parseLong("".equals(line[5]) ? "0" : line[5]);
                    int protocol = Integer.parseInt("".equals(line[9]) ? "0" : line[9]);
                    String appCompany = line[10];

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

                    newAppmap.put(appSketchLabel.getAppName(), appSketchLabel);
                } catch (Exception lineException) {
                    System.out.println("AppUtils line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            appmap = newAppmap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
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