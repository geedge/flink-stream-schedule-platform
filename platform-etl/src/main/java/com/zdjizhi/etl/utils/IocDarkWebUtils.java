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

    private static HashMap<String, Long> tagRuleIdMap = new HashMap<>();

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
                cnRecordLog.getRule_id_list().add(tagRuleIdMap.get(ipTag));
                cnRecordLog.getIoc_type_list().add("ip");
            }
            cnRecordLog.getServer_ip_tags().add(ipTag);
        }
        String domainTag = getDomainTag(cnRecordLog.getDomain());
        if (domainTag != null) {
            if (tagRuleIdMap.containsKey(domainTag)) {
                cnRecordLog.getRule_id_list().add(tagRuleIdMap.get(domainTag));
                cnRecordLog.getIoc_type_list().add("domain");
            }
            cnRecordLog.getDomain_tags().add(domainTag);
        }
    }

    public static void updateInternalRuleId(Long key, RuleInfo ruleInfo) {
        tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
    }

    public static void removeInternalRuleId(Long key, RuleInfo ruleInfo) {
        tagRuleIdMap.remove(ruleInfo.getName());
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
