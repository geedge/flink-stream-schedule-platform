package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IocDarkWebUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IocDarkWebUtils.class);

    private static HashMap<String, String> ipMap = new HashMap<>();
    private static HashMap<String, String> domainMap = new HashMap<>();

    private static TreeMap<String, Long> tagRuleIdMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public static void readIocDarkWebCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, String> newIpMap = new HashMap<>((int)(csvReader.getLineNumber() / 0.75F + 1.0F));
            HashMap<String, String> newDomainMap = new HashMap<>((int)(csvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String type = line.get("ioc_type");
                    String value = line.get("ioc_value");
                    String nodeType = line.get("node_type");

                    if ("ip".equals(type)) {
                        newIpMap.put(value, nodeType);
                    } else if ("domain".equals(type)) {
                        newDomainMap.put(value, nodeType);
                    }
                } catch (Exception lineException) {
                    System.out.println("DnsServerUtils line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            ipMap = newIpMap;
            domainMap = newDomainMap;
        } catch (Exception e) {
            LOG.error("dns server init error", e);
        }
    }


    public static void setIocDarkWebInfo(CnRecordLog cnRecordLog) {
        String serverIp = cnRecordLog.getCommon_server_ip();
        if (ipMap.containsKey(serverIp)) {
            String ipTag = ipMap.get(serverIp);
            if (tagRuleIdMap.containsKey(ipTag)) {
                cnRecordLog.putRuleIdAndIocType(tagRuleIdMap.get(ipTag), "ip");
            }
            cnRecordLog.putServer_ip_tag(ipTag);
        }
        String domainTag = getDomainTag(cnRecordLog.getDomain());
        if (domainTag != null) {
            if (tagRuleIdMap.containsKey(domainTag)) {
                cnRecordLog.putRuleIdAndIocType(tagRuleIdMap.get(domainTag), "domain");
            }
            cnRecordLog.putDomain_tag(domainTag);
        }
    }

    public static void updateInternalRuleId(Long key, RuleInfo ruleInfo) {
        if (tagRuleIdMap.containsValue(ruleInfo.getRuleId())) {
            if (tagRuleIdMap.containsKey(ruleInfo.getName())) {
                //id and name not change
                if (tagRuleIdMap.get(ruleInfo.getName()) != ruleInfo.getRuleId()) {
                    System.out.println(TimeUtils.getCurrentTime() + ": ioc dark web warn: the name " + ruleInfo.getName() + " have been used by rule id " + tagRuleIdMap.get(ruleInfo.getName()));
                    for (String ruleName : tagRuleIdMap.keySet()) {
                        if (tagRuleIdMap.get(ruleName) == ruleInfo.getRuleId()) {
                            tagRuleIdMap.remove(ruleName);
                            break;
                        }
                    }
                    tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
                } else {
                    System.out.println(TimeUtils.getCurrentTime() + ": ioc dark web receive the rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
                }
            } else {
                tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
                for (String ruleName : tagRuleIdMap.keySet()) {
                    if (tagRuleIdMap.get(ruleName) == ruleInfo.getRuleId()) {
                        tagRuleIdMap.remove(ruleName);
                        break;
                    }
                }
                System.out.println(TimeUtils.getCurrentTime() + ": ioc dark web update rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
            }
        } else {
            if (tagRuleIdMap.containsKey(ruleInfo.getName())) {
                System.out.println(TimeUtils.getCurrentTime() + ": ioc dark web warn: the name " + ruleInfo.getName() + " have been used by rule id " + tagRuleIdMap.get(ruleInfo.getName()));
            } else {
                System.out.println(TimeUtils.getCurrentTime() + ": ioc dark web add rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
            }
            tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
        }
    }

    public static void removeInternalRuleId(Long key, RuleInfo ruleInfo) {
        tagRuleIdMap.remove(ruleInfo.getName());
        System.out.println(TimeUtils.getCurrentTime() + ": ioc dark web remove rule : " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
    }

    private static String getDomainTag(String domain) {
        if (domain == null) {
            return null;
        }
        if (domainMap.containsKey(domain)) {
            return domainMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getDomainTag(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public static void close() {
        ipMap.clear();
        ipMap = null;
        domainMap.clear();
        domainMap = null;
    }
}
