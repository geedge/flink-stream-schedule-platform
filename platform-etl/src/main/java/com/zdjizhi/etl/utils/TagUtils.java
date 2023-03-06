package com.zdjizhi.etl.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FileByteUtils;
import com.zdjizhi.base.utils.HdfsUtils;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TagUtils {

    private static Logger LOG = LoggerFactory.getLogger(TagUtils.class);

    private static Logger logger = LoggerFactory.getLogger(TagUtils.class);

    private static Map<String, Map<String, List<String>>> ipTagRules = new HashMap<>();
    private static Map<String, Map<String, List<String>>> fqdnTagRules = new HashMap<>();
    private static Map<String, Map<String, List<String>>> appLabelTagRules = new HashMap<>();

    private static String ipFileName = "test_ip.txt"; //todo ipFileName具体名字
    private static String fqdnFileName = "test_fqdn.txt"; //todo fqdnFileName具体名字
    private static String appLabelFileName = "test_appLabel.txt"; //todo appLabelFileName具体名字

    public static void loadTag() {
        try {
            byte[] ipFileBytes;
            byte[] fqdnFileBytes;
            byte[] appLabelFileBytes;

            if ("CLUSTER".equals(CommonConfig.CLUSTER_OR_SINGLE)) {
                ipFileBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + ipFileName);
                fqdnFileBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + fqdnFileName);
                appLabelFileBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + appLabelFileName);
            } else {
                ipFileBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + ipFileName);
                fqdnFileBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + fqdnFileName);
                appLabelFileBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + appLabelFileName);
            }
            if (ipFileBytes != null) {
                parseIpTagRules(ipFileName, new String(ipFileBytes));
            }
            if (fqdnFileBytes != null) {
                parseFqdnTagRules(fqdnFileName, new String(fqdnFileBytes));
            }
            if (appLabelFileBytes != null) {
                parseAppLabelTagRules(appLabelFileName, new String(appLabelFileBytes));
            }
        } catch (Exception e) {
            throw new RuntimeException("初始化Tag失败", e);
        }

    }

    public static void updateTag(Map<String, byte[]> knowledgeFileCache) {
        try {
            for (String key : knowledgeFileCache.keySet()) {
                String[] split = key.split(",");
                if (split.length != 2) {
                    continue;
                }
                String first = split[0];
                String fileName = split[1];
                byte[] context = knowledgeFileCache.get(key);
                if ("ip_user_defined".equals(first)) {
                    parseIpTagRules(fileName,new String(context));
                }
                if ("domain_user_defined".equals(first)) {
                    parseFqdnTagRules(fileName,new String(context));
                }
                if ("app_user_defined".equals(first)) {
                    parseAppLabelTagRules(fileName,new String(context));
                }
            }
            logger.info("tag更新成功");
        } catch (Exception e) {
            logger.error("更新tag失败", e);
        }
    }

    private static void parseIpTagRules(String ipFileName, String ipFileContent) {
        if (JSON.isValidObject(ipFileContent)) {
            Map<String, List<String>> ipTags = new HashMap<>();
            JSONObject jsonObject = JSON.parseObject(ipFileContent);
            String tagType = jsonObject.getString("tagType");
            if (!"ip".equals(tagType)) {
                logger.warn("解析ip类型时获取到了错误的数据类型：" + tagType);
            }
            JSONArray data = jsonObject.getJSONArray("data");
            for (Object datum : data) {
                JSONObject tagInfo = (JSONObject) datum;
                String tagValue = tagInfo.getString("tagValue");
                JSONArray itemList = tagInfo.getJSONArray("itemList");
                ipTags.put(tagValue,itemList.toJavaList(String.class));
            }
            ipTagRules.put(ipFileName,ipTags);
        } else {
            logger.error("错误的json内容：" + ipFileContent);
        }

    }

    private static void parseFqdnTagRules(String fqdnFileName, String fqdnFileContent) {
        if (JSON.isValidObject(fqdnFileContent)) {
            Map<String, List<String>> fqdnTags = new HashMap<>();
            JSONObject jsonObject = JSON.parseObject(fqdnFileContent);
            String tagType = jsonObject.getString("tagType");
            if (!"domain".equals(tagType)) {
                logger.warn("解析fqdn类型时获取到了错误的数据类型：" + tagType);
            }
            JSONArray data = jsonObject.getJSONArray("data");
            for (Object datum : data) {
                JSONObject tagInfo = (JSONObject) datum;
                String tagValue = tagInfo.getString("tagValue");
                JSONArray itemList = tagInfo.getJSONArray("itemList");
                fqdnTags.put(tagValue,itemList.toJavaList(String.class));
            }
            fqdnTagRules.put(fqdnFileName,fqdnTags);
        } else {
            logger.error("错误的json内容：" + fqdnFileContent);
        }

    }

    private static void parseAppLabelTagRules(String appLabelFileName, String appLabelFileContent) {
        if (JSON.isValidObject(appLabelFileContent)) {
            Map<String, List<String>> appTags = new HashMap<>();
            JSONObject jsonObject = JSON.parseObject(appLabelFileContent);
            String tagType = jsonObject.getString("tagType");
            if (!"app".equals(tagType)) {
                logger.warn("解析appLabel类型时获取到了错误的数据类型：" + tagType);
            }
            JSONArray data = jsonObject.getJSONArray("data");
            for (Object datum : data) {
                JSONObject tagInfo = (JSONObject) datum;
                String tagValue = tagInfo.getString("tagValue");
                JSONArray itemList = tagInfo.getJSONArray("itemList");
                appTags.put(tagValue,itemList.toJavaList(String.class));
            }
            appLabelTagRules.put(appLabelFileName,appTags);
        } else {
            logger.error("错误的json内容：" + appLabelFileContent);
        }

    }

    public static void setTag(CnRecordLog entity) {
        try {
            setIpTags(entity);
            setFqdnTags(entity);
            setAppLabelTags(entity);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }

    }

    private static void setIpTags(CnRecordLog entity) {
        entity.setClient_ip_tags(getIpTags(entity.getCommon_client_ip()));
        entity.setServer_ip_tags(getIpTags(entity.getCommon_server_ip()));
    }

    private static List<String> getIpTags(String ip) {
        List<String> tags = new ArrayList<>();
        for (String fileName : ipTagRules.keySet()) {
            Map<String, List<String>> fileMap = ipTagRules.get(fileName);
            for (String tag : fileMap.keySet()) {
                List<String> rules = fileMap.get(tag);
                for (String rule : rules) {
                    IPAddressString ipAddressString = new IPAddressString(rule);
                    TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();
                    if (ipAddressString.isIPAddress()) {
                        IPAddress address = ipAddressString.getAddress();
                        IPAddress lower;
                        IPAddress upper;
                        if (address.isPrefixed()) {
                            lower = address.getLower();
                            upper = address.getUpper();
                            if (!address.isMultiple()) {
                                lower = address.adjustPrefixLength(address.getBitCount());
                                upper = address.toMaxHost().withoutPrefixLength();
                            }
                            treeRangeSet.add(Range.closed(lower, upper));
                        } else {
                            treeRangeSet.add(Range.closed(address,address));
                        }
                    }
                    IPAddress address = new IPAddressString(ip).getAddress();
                    if (treeRangeSet.contains(address)) {
                        tags.add(tag);
                        break;
                    }
                }
            }
        }
        return tags;
    }

    private static void setFqdnTags(CnRecordLog entity) {
        entity.setDomain_tags(getFqdnTags(entity.getDomain()));
        entity.setHttp_host_tags(getFqdnTags(entity.getHttp_host()));
        entity.setSsl_sni_tags(getFqdnTags(entity.getSsl_sni()));
    }

    private static List<String> getFqdnTags(String fqdn) {
        List<String> tags = new ArrayList<>();
        for (String fileName : fqdnTagRules.keySet()) {
            Map<String, List<String>> fileMap = fqdnTagRules.get(fileName);
            for (String tag : fileMap.keySet()) {
                List<String> rules = fileMap.get(tag);
                for (String rule : rules) {
                    if (isMatch(rule, fqdn)) {
                        tags.add(tag);
                        break;
                    }
                }
            }
        }
        return tags;
    }

    private static void setAppLabelTags(CnRecordLog entity) {
        entity.setApp_tags(getAppLabelTags(entity.getCommon_app_label()));
    }

    private static List<String> getAppLabelTags(String appLable) {
        List<String> tags = new ArrayList<>();
        if (appLable == null) {
            return tags;
        }
        for (String fileName : appLabelTagRules.keySet()) {
            Map<String, List<String>> fileMap = appLabelTagRules.get(fileName);
            for (String tag : fileMap.keySet()) {
                List<String> rules = fileMap.get(tag);
                for (String rule : rules) {
                    if (appLable.equals(rule)) {
                        tags.add(tag);
                        break;
                    }
                }
            }
        }
        return tags;
    }

    private static boolean isMatch(String rule, String context) {
        if (rule == null) {
            return false;
        }
        if (rule.startsWith("$") || rule.startsWith("*")) {
            String newRule = rule.substring(1);
            if (rule.startsWith("*") && context != null && context.endsWith(newRule)) {
                return true;
            }
            if (rule.startsWith("$") && newRule.equals(context)) {
                return true;
            }
        }
        return false;
    }

}
