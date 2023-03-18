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

import java.util.*;

public class TagUtils {

    private static Logger LOG = LoggerFactory.getLogger(TagUtils.class);

    private static Logger logger = LoggerFactory.getLogger(TagUtils.class);

    private static Map<String, Map<String, TreeRangeSet<IPAddress>>> ipTagRules = new HashMap<>();
    private static Map<String, Map<Integer, Map<String,List<String>>>> fqdnTagFuzzyMatchRules = new HashMap<>();
    private static Map<String, Map<String, List<String>>> fqdnTagFullMatchRules = new HashMap<>();
    private static Map<String, Map<String, List<String>>> appLabelTagRules = new HashMap<>();

    private static String ipFileName = "test_ip.txt"; //todo ipFileName具体名字
    private static String fqdnFileName = "test_fqdn.txt"; //todo fqdnFileName具体名字
    private static String appLabelFileName = "test_appLabel.txt"; //todo appLabelFileName具体名字

    public static void loadTag() {
        try {
            byte[] ipFileBytes = null;
            byte[] fqdnFileBytes = null;
            byte[] appLabelFileBytes = null;

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
            System.out.println("初始化tag成功");
        } catch (Exception e) {
            throw new RuntimeException("初始化Tag失败", e);
        }

    }

    public static void updateTag(Map<String, byte[]> knowledgeFileCache) {
        try {
            ipTagRules.clear();
            fqdnTagFuzzyMatchRules.clear();
            fqdnTagFullMatchRules.clear();
            appLabelTagRules.clear();
            for (String key : knowledgeFileCache.keySet()) {
                System.out.println("开始更新tag文件:" + key);
                String[] split = key.split(",");
                if (split.length != 2) {
                    System.out.println("文件：" + key + " 已忽略");
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
        try {
            if (JSON.isValidObject(ipFileContent)) {
                Map<String, TreeRangeSet<IPAddress>> ipTags = new HashMap<>();
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
                    if (itemList == null) {
                        System.out.println("tag:" + tagValue + " 对应的规则列表为空");
                        continue;
                    }
                    List<String> rules = itemList.toJavaList(String.class);
                    TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();
                    for (String rule : rules) {
                        IPAddressString ipAddressString = new IPAddressString(rule);
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
                    }
                    ipTags.put(tagValue,treeRangeSet);
                }
                ipTagRules.put(ipFileName,ipTags);
                System.out.println("解析tag文件" + ipFileName + "完成，已经转成" + ipTags.size() + "个ip内置规则");
            } else {
                logger.error("错误的json内容：" + ipFileContent);
            }
        } catch (Exception e) {
            logger.error("parse ip rules " + ipFileName + " failed", e);
        }


    }

    private static void parseFqdnTagRules(String fqdnFileName, String fqdnFileContent) {
        try {
            if (JSON.isValidObject(fqdnFileContent)) {
                Map<Integer, Map<String,List<String>>> fqdnTagsFuzzy = new HashMap<>();
                Map<String, List<String>> fqdnTagsFull = new HashMap<>();
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
                    if (itemList == null) {
                        System.out.println("tag:" + tagValue + " 对应的规则列表为空");
                        continue;
                    }
                    List<String> rules = itemList.toJavaList(String.class);
                    List<String> newRules = new ArrayList<>(rules.size());
                    for (String rule : rules) {
                        if (rule == null || rule.length() == 0) {
                            continue;
                        }
                        if (!rule.startsWith("$") && !rule.startsWith("*")) {
                            continue;
                        }
                        String newRule = rule.substring(1);
                        if (rule.startsWith("*")){
                            if (fqdnTagsFuzzy.containsKey(newRule.length())) {
                                Map<String, List<String>> lengthTagMap = fqdnTagsFuzzy.get(newRule.length());
                                if (lengthTagMap.containsKey(newRule)) {
                                    List<String> tags = lengthTagMap.get(newRule);
                                    tags.add(tagValue);
                                } else {
                                    List<String> tags = new ArrayList<>();
                                    tags.add(tagValue);
                                    lengthTagMap.put(newRule,tags);
                                }
                            } else {
                                Map<String, List<String>> lengthTagMap = new HashMap<>();
                                List<String> tags = new ArrayList<>();
                                tags.add(tagValue);
                                lengthTagMap.put(newRule, tags);
                                fqdnTagsFuzzy.put(newRule.length(), lengthTagMap);
                            }
                        } else {
                            if (fqdnTagsFull.containsKey(newRule)) {
                                fqdnTagsFull.get(newRule).add(tagValue);
                            } else {
                                List<String> tags = new ArrayList<>();
                                tags.add(tagValue);
                                fqdnTagsFull.put(newRule,tags);
                            }
                        }
                    }
                }
                fqdnTagFullMatchRules.put(fqdnFileName,fqdnTagsFull);
                fqdnTagFuzzyMatchRules.put(fqdnFileName,fqdnTagsFuzzy);
                System.out.println("解析tag文件" + fqdnFileName + "完成，已经转成" + fqdnTagsFull.keySet().size() + "个domain全匹配内置规则和" + fqdnTagsFuzzy.keySet().size() + "个domain模糊内置规则");
            } else {
                logger.error("错误的json内容：" + fqdnFileContent);
            }
        } catch (Exception e) {
            logger.error("parse fqdn rules " + fqdnFileName + " failed", e);
        }


    }

    private static void parseAppLabelTagRules(String appLabelFileName, String appLabelFileContent) {
        try {
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
                    if (itemList == null) {
                        System.out.println("tag:" + tagValue + " 对应的规则列表为空");
                        continue;
                    }
                    List<String> rules = itemList.toJavaList(String.class);
                    for (String rule : rules) {
                        if (appTags.containsKey(rule)) {
                            appTags.get(rule).add(tagValue);
                        } else {
                            List<String> tags = new ArrayList<String>();
                            tags.add(tagValue);
                            appTags.put(rule, tags);
                        }
                    }
                }
                appLabelTagRules.put(appLabelFileName,appTags);
                System.out.println("解析tag文件" + appLabelFileName + "完成，已经转成" + appTags.keySet().size() + "个app内置规则");
            } else {
                logger.error("错误的json内容：" + appLabelFileContent);
            }
        } catch (Exception e) {
            logger.error("parse app rules " + appLabelFileName + " failed", e);
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
            Map<String, TreeRangeSet<IPAddress>> tagMap = ipTagRules.get(fileName);
            for (String tag : tagMap.keySet()) {
                TreeRangeSet<IPAddress> treeRangeSet = tagMap.get(tag);
                IPAddress address = new IPAddressString(ip).getAddress();
                if (treeRangeSet.contains(address)) {
                    tags.add(tag);
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
        for (String fileName : fqdnTagFuzzyMatchRules.keySet()) {
            Map<Integer, Map<String, List<String>>> lengthTagMap = fqdnTagFuzzyMatchRules.get(fileName);
            for (Integer length : lengthTagMap.keySet()) {
                if (fqdn == null || fqdn.length() < length) {
                    continue;
                }
                String suffix = fqdn.substring(fqdn.length() - length);
                if (lengthTagMap.get(length).containsKey(suffix)) {
                    tags.addAll(lengthTagMap.get(length).get(suffix));
                }
            }
        }
        for (String fileName : fqdnTagFullMatchRules.keySet()) {
            Map<String, List<String>> ruleMap = fqdnTagFullMatchRules.get(fileName);
            if (ruleMap.containsKey(fqdn)) {
                tags.addAll(ruleMap.get(fqdn));
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
            Map<String, List<String>> ruleMap = appLabelTagRules.get(fileName);
            if (ruleMap.containsKey(appLable)) {
                return ruleMap.get(appLable);
            }
        }
        return tags;
    }

    private static boolean isMatch(String rule, String context) {
        return context != null && context.endsWith(rule);
    }

}
