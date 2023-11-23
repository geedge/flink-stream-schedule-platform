package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppTagUtils {

    private static final Logger logger = LoggerFactory.getLogger(AppTagUtils.class);

    private static Map<String, Node> appLabelTagRules = new HashMap<>();

    public static void updateTag(Map<Long, byte[]> contents) {
        Map<String, Node> newAppLabelTagRules = new HashMap<>();
        for (Long s : contents.keySet()) {
            parseAppLabelTagRules(newAppLabelTagRules, s, contents.get(s));
        }
        appLabelTagRules = newAppLabelTagRules;
    }

    public static void setTag(CnRecordLog entity) {
        try {
            Node appLabelNode = getAppLabelNode(entity.getCommon_app_label());
            entity.putApp_tags(appLabelNode.getTags());
            BaseTagUtils.setRuleIdAndIocType(entity, appLabelNode);
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static Node getAppLabelNode(String appLable) {
        if (appLable == null || appLable.length() == 0) {
            return new Node();
        }

        if (appLabelTagRules.containsKey(appLable)) {
            return appLabelTagRules.get(appLable);
        }

        return new Node();
    }

    private static void parseAppLabelTagRules(Map<String, Node> appTags, Long id, byte[] appLabelFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(appLabelFileContent));
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("app_name");
            HighCsvReader csvReader = new HighCsvReader(reader, needColumns);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                String tagValue = line.get("tag_value");
                String appName = line.get("app_name");

                if (appTags.containsKey(appName)) {
                    Node node = appTags.get(appName);
                    node.getTags().add(tagValue);
                    node.put(id, "app");
                } else {
                    Node node = new Node();
                    node.getTags().add(tagValue);
                    node.put(id, "app");
                    appTags.put(appName, node);
                }
            }
        } catch (Exception e) {
            logger.error("parse app rules id" + id + " failed", e);
        }
    }
}
