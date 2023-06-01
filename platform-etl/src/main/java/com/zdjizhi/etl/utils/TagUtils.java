package com.zdjizhi.etl.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
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

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class TagUtils {

    private static final Logger logger = LoggerFactory.getLogger(TagUtils.class);

    private static TreeRangeMap<IPAddress, List<String>> ipTagRules = TreeRangeMap.create();
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
                if (CommonInternalConfig.IP_TAG_TYPE.equals(first)) {
                    parseIpTagRules(newIpTagRules, fileName, context);
                }
                if (CommonInternalConfig.DOMAIN_TAG_TYPE.equals(first)) {
                    parseFqdnTagRules(newFqdnTagFuzzyMatchRules, newFqdnTagFullMatchRules, fileName, context);
                }
                if (CommonInternalConfig.APP_TAG_TYPE.equals(first)) {
                    parseAppLabelTagRules(newAppLabelTagRules, fileName, context);
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

    private static void parseIpTagRules(TreeRangeMap<IPAddress, List<String>> treeRangeMap, String ipFileName, byte[] ipFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(ipFileContent));
            CsvReaderUtils csvReader = new CsvReaderUtils(reader);
            List<Map<String, String>> data = csvReader.readAll();
            for (Map<String, String> line : data) {
                try {
                    String tagValue = line.get("tag_value");
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");

                    IPAddress startIpAddress = null;
                    IPAddress endIpAddress = null;
                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        if (ipAddress == null) {
                            continue;
                        }
                        startIpAddress = ipAddress;
                        endIpAddress = ipAddress;
                    } else if ("Range".equals(addrFormat)) {
                        IPAddress ipAddress1 = new IPAddressString(ip1).getAddress();
                        IPAddress ipAddress2 = new IPAddressString(ip2).getAddress();
                        if (ipAddress1 == null || ipAddress2 == null) {
                            continue;
                        }
                        startIpAddress = ipAddress1;
                        endIpAddress = ipAddress2;
                    } else if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        IPAddress ipAddressLower = cidrIpAddress.getLower();
                        IPAddress ipAddressUpper = cidrIpAddress.getUpper();
                        startIpAddress = ipAddressLower;
                        endIpAddress = ipAddressUpper;
                    } else {
                        System.out.println("unknown addrFormat: " + addrFormat);
                        continue;
                    }

                    RangeMap<IPAddress, List<String>> ipAddressListRangeMap = treeRangeMap.subRangeMap(Range.closed(startIpAddress, endIpAddress));
                    Map<Range<IPAddress>, List<String>> rangeListMap = ipAddressListRangeMap.asMapOfRanges();
                    TreeRangeMap<IPAddress, List<String>> subRangeMap = TreeRangeMap.create();
                    List<String> tags = new ArrayList<>();
                    tags.add(tagValue);
                    subRangeMap.put(Range.closed(startIpAddress, endIpAddress), tags);
                    for (Range<IPAddress> ipAddressRange : rangeListMap.keySet()) {
                        List<String> ipAddressTags = ipAddressListRangeMap.get(ipAddressRange.lowerEndpoint());
                        List<String> newTags = new ArrayList<>();
                        if (ipAddressTags != null) {
                            newTags.addAll(ipAddressTags);
                        }
                        newTags.add(tagValue);
                        subRangeMap.put(ipAddressRange, newTags);
                    }
                    treeRangeMap.putAll(subRangeMap);
                } catch (Exception lineException) {
                    System.out.println("ip tag line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
        } catch (Exception e) {
            logger.error("parse ip rules " + ipFileName + " failed", e);
        }


    }

    private static void parseFqdnTagRules(Map<Integer, Map<String,List<String>>> fqdnTagsFuzzy, Map<String, List<String>> fqdnTagsFull, String fqdnFileName, byte[] fqdnFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(fqdnFileContent));
            CsvReaderUtils csvReader = new CsvReaderUtils(reader);
            List<Map<String, String>> data = csvReader.readAll();
            for (Map<String, String> line : data) {
                String tagValue = line.get("tag_value");
                String rule = line.get("domain");

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
        } catch (Exception e) {
            logger.error("parse fqdn rules " + fqdnFileName + " failed", e);
        }


    }

    private static void parseAppLabelTagRules(Map<String, List<String>> appTags, String appLabelFileName, byte[] appLabelFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(appLabelFileContent));
            CsvReaderUtils csvReader = new CsvReaderUtils(reader);
            List<Map<String, String>> data = csvReader.readAll();
            for (Map<String, String> line : data) {
                String tagValue = line.get("tag_value");
                String appName = line.get("app_name");

                if (appTags.containsKey(appName)) {
                    appTags.get(appName).add(tagValue);
                } else {
                    List<String> tags = new ArrayList<String>();
                    tags.add(tagValue);
                    appTags.put(appName, tags);
                }
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
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }

    }

    private static void setIpTags(CnRecordLog entity) {
        entity.setClient_ip_tags(getIpTags(entity.getCommon_client_ip()));
        entity.setServer_ip_tags(getIpTags(entity.getCommon_server_ip()));
    }

    private static List<String> getIpTags(String ip) {
        List<String> tags = new ArrayList<>();
        IPAddress address = new IPAddressString(ip).getAddress();
        if (address != null) {
            List<String> addressTags = ipTagRules.get(address);
            if (addressTags != null) {
                tags.addAll(addressTags);
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
