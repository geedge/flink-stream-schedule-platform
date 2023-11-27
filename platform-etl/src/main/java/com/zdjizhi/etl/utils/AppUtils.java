package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.AppSketchLabel;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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
                String appCompany = line.get("app_company");

                AppSketchLabel appSketchLabel = new AppSketchLabel();
                appSketchLabel.setAppName(appName);
                appSketchLabel.setCategory(category);
                appSketchLabel.setSubCategory(subCategory);
                appSketchLabel.setAppCompany(appCompany);

                newAppMap.put(appSketchLabel.getAppName(), appSketchLabel);
            } catch (Exception lineException) {
                LOG.error("AppUtils line: " + line.toString() + " parse error：" + lineException, lineException);
            }
        }
        appMap = newAppMap;
    }

    public static void setAppSketchMessage(CnRecordLog entity) {
        try {
            String appName = entity.getCommon_app_label();
            if (!"unknown".equals(appName)){
                if (appMap.containsKey(appName)) {
                    AppSketchLabel appSketchLabel = appMap.get(appName);
                    entity.setApp_subcategory(appSketchLabel.getSubCategory());
                    entity.setApp_category(appSketchLabel.getCategory());
                    entity.setApp_company(appSketchLabel.getAppCompany());
                }
            }else {
                entity.setCommon_app_label(null);
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    public static void close() {
        appMap.clear();
        appMap = null;
    }
}