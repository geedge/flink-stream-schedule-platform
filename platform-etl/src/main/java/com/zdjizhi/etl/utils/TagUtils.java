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
import org.apache.flink.shaded.guava18.com.google.common.collect.RangeMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TagUtils {

    private static Logger LOG = LoggerFactory.getLogger(TagUtils.class);

    private static Logger logger = LoggerFactory.getLogger(TagUtils.class);

    private static TreeRangeMap<IPAddress, List<String>> ipTagRules;
    private static Map<Integer, Map<String,List<String>>> fqdnTagFuzzyMatchRules = new HashMap<>();
    private static Map<String, List<String>> fqdnTagFullMatchRules = new HashMap<>();
    private static Map<String, List<String>> appLabelTagRules = new HashMap<>();

    public static void updateTag(Map<String, byte[]> knowledgeFileCache) {
        try {
            TreeRangeMap<IPAddress, List<String>> newIpTagRules = TreeRangeMap.create();
            Map<Integer, Map<String,List<String>>> newFqdnTagFuzzyMatchRules = new HashMap<>();
            Map<String, List<String>> newFqdnTagFullMatchRules = new HashMap<>();
            Map<String, List<String>> newAppLabelTagRules = new HashMap<>();

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
                    parseIpTagRules(newIpTagRules, fileName, new String(context));
                }
                if ("domain_user_defined".equals(first)) {
                    parseFqdnTagRules(newFqdnTagFuzzyMatchRules, newFqdnTagFullMatchRules, fileName, new String(context));
                }
                if ("app_user_defined".equals(first)) {
                    parseAppLabelTagRules(newAppLabelTagRules, fileName, new String(context));
                }
            }
            System.out.println("解析 ip tags 文件完成，已经转成" + newIpTagRules.asMapOfRanges().size() + "个ip内置规则");
            System.out.println("解析 domain tags 文件完成，已经转成" + newFqdnTagFullMatchRules.keySet().size() + "个domain全匹配内置规则和" + newFqdnTagFuzzyMatchRules.keySet().size() + "个domain模糊内置规则");
            System.out.println("解析 app tags 文件完成，已经转成" + newAppLabelTagRules.keySet().size() + "个app内置规则");
            ipTagRules = newIpTagRules;
            fqdnTagFuzzyMatchRules = newFqdnTagFuzzyMatchRules;
            fqdnTagFullMatchRules = newFqdnTagFullMatchRules;
            appLabelTagRules = newAppLabelTagRules;
            logger.info("tag更新成功");
        } catch (Exception e) {
            logger.error("更新tag失败", e);
        }
    }

    private static void parseIpTagRules(TreeRangeMap<IPAddress, List<String>> treeRangeMap, String ipFileName, String ipFileContent) {
        try {
            if (JSON.isValidObject(ipFileContent)) {
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

                            } else {
                                lower = address;
                                upper = address;
                            }
                            RangeMap<IPAddress, List<String>> ipAddressListRangeMap = treeRangeMap.subRangeMap(Range.closed(lower, upper));
                            Map<Range<IPAddress>, List<String>> rangeListMap = ipAddressListRangeMap.asMapOfRanges();
                            TreeRangeMap<IPAddress, List<String>> subRangeMap = TreeRangeMap.create();
                            List<String> tags = new ArrayList<>();
                            tags.add(tagValue);
                            subRangeMap.put(Range.closed(lower, upper), tags);
                            for (Range<IPAddress> ipAddressRange : rangeListMap.keySet()) {
                                List<String> ipAddressTags = ipAddressListRangeMap.get(ipAddressRange.lowerEndpoint());
                                List<String> newTags = new ArrayList<>(ipAddressTags);
                                newTags.add(tagValue);
                                subRangeMap.put(ipAddressRange, newTags);
                            }
                            treeRangeMap.putAll(subRangeMap);
                        }
                    }
                }
            } else {
                logger.error("错误的json内容：" + ipFileContent);
            }
        } catch (Exception e) {
            logger.error("parse ip rules " + ipFileName + " failed", e);
        }


    }

    private static void parseFqdnTagRules(Map<Integer, Map<String,List<String>>> fqdnTagsFuzzy, Map<String, List<String>> fqdnTagsFull, String fqdnFileName, String fqdnFileContent) {
        try {
            if (JSON.isValidObject(fqdnFileContent)) {
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
            } else {
                logger.error("错误的json内容：" + fqdnFileContent);
            }
        } catch (Exception e) {
            logger.error("parse fqdn rules " + fqdnFileName + " failed", e);
        }


    }

    private static void parseAppLabelTagRules(Map<String, List<String>> appTags, String appLabelFileName, String appLabelFileContent) {
        try {
            if (JSON.isValidObject(appLabelFileContent)) {
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
        IPAddress address = new IPAddressString(ip).getAddress();
        List<String> addressTags = ipTagRules.get(address);
        if (addressTags != null) {
            tags.addAll(addressTags);
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
        if (fqdn == null || fqdn.length() == 0) {
            return tags;
        }

        for (Integer length : fqdnTagFuzzyMatchRules.keySet()) {
            if (fqdn.length() < length) {
                continue;
            }
            String suffix = fqdn.substring(fqdn.length() - length);
            if (fqdnTagFuzzyMatchRules.get(length).containsKey(suffix)) {
                tags.addAll(fqdnTagFuzzyMatchRules.get(length).get(suffix));
            }
        }

        if (fqdnTagFullMatchRules.containsKey(fqdn)) {
            tags.addAll(fqdnTagFullMatchRules.get(fqdn));
        }

        return tags;
    }

    private static void setAppLabelTags(CnRecordLog entity) {
        entity.setApp_tags(getAppLabelTags(entity.getCommon_app_label()));
    }

    private static List<String> getAppLabelTags(String appLable) {
        List<String> tags = new ArrayList<>();
        if (appLable == null || appLable.length() == 0) {
            return tags;
        }

        if (appLabelTagRules.containsKey(appLable)) {
            return appLabelTagRules.get(appLable);
        }

        return tags;
    }

    private static boolean isMatch(String rule, String context) {
        return context != null && context.endsWith(rule);
    }

}
