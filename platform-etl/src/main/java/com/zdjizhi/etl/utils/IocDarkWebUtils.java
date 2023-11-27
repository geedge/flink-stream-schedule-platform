package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IocDarkWebUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IocDarkWebUtils.class);

    private static HashMap<String, String> ipMap = new HashMap<>();
    private static HashMap<String, String> domainMap = new HashMap<>();

    private static final HashMap<String, Long> tagRuleIdMap = new HashMap<>();

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
                    LOG.error("DnsServerUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            ipMap = newIpMap;
            domainMap = newDomainMap;
        } catch (Exception e) {
            LOG.error("dns server init error", e);
        }
    }


    public static void setIocDarkWebInfo(CnRecordLog cnRecordLog) {
        try {
            String serverIp = cnRecordLog.getCommon_server_ip();
            if (ipMap.containsKey(serverIp)) {
                String ipTag = ipMap.get(serverIp);
                String ipTagLowerCase = ipTag.toLowerCase();
                if (tagRuleIdMap.containsKey(ipTagLowerCase)) {
                    cnRecordLog.putRuleIdAndIocType(tagRuleIdMap.get(ipTagLowerCase), "ip");
                }
                cnRecordLog.putServer_ip_tag(ipTag);
            }
            String domainTag = getDomainTag(cnRecordLog.getDomain());
            if (domainTag != null) {
                String domainTagLowerCase = domainTag.toLowerCase();
                if (tagRuleIdMap.containsKey(domainTagLowerCase)) {
                    cnRecordLog.putRuleIdAndIocType(tagRuleIdMap.get(domainTagLowerCase), "domain");
                }
                cnRecordLog.putDomain_tag(domainTag);
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }

    public static void updateInternalRuleId(Long key, RuleInfo ruleInfo) {
        ruleInfo.setName(ruleInfo.getName().toLowerCase());
        if (tagRuleIdMap.containsValue(ruleInfo.getRuleId())) {
            if (tagRuleIdMap.containsKey(ruleInfo.getName())) {
                //id and name not change
                if (tagRuleIdMap.get(ruleInfo.getName()) != ruleInfo.getRuleId()) {
                    LOG.info("ioc dark web warn: the name " + ruleInfo.getName() + " have been used by rule id " + tagRuleIdMap.get(ruleInfo.getName()));
                    for (String ruleName : tagRuleIdMap.keySet()) {
                        if (tagRuleIdMap.get(ruleName) == ruleInfo.getRuleId()) {
                            tagRuleIdMap.remove(ruleName);
                            break;
                        }
                    }
                    tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
                } else {
                    LOG.info("ioc dark web receive the rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
                }
            } else {
                tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
                for (String ruleName : tagRuleIdMap.keySet()) {
                    if (tagRuleIdMap.get(ruleName) == ruleInfo.getRuleId()) {
                        tagRuleIdMap.remove(ruleName);
                        break;
                    }
                }
                LOG.info("ioc dark web update rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
            }
        } else {
            if (tagRuleIdMap.containsKey(ruleInfo.getName())) {
                LOG.info("ioc dark web warn: the name " + ruleInfo.getName() + " have been used by rule id " + tagRuleIdMap.get(ruleInfo.getName()));
            } else {
                LOG.info("ioc dark web add rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
            }
            tagRuleIdMap.put(ruleInfo.getName(), ruleInfo.getRuleId());
        }
    }

    public static void removeInternalRuleId(Long key, RuleInfo ruleInfo) {
        tagRuleIdMap.remove(ruleInfo.getName().toLowerCase());
        LOG.info("ioc dark web remove rule : " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
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
