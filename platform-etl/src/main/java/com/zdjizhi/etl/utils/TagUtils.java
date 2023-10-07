package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.RangeMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class TagUtils {

    private static final Logger logger = LoggerFactory.getLogger(TagUtils.class);

    private static TreeRangeMap<IPAddress, Node> ipTagRules = TreeRangeMap.create();
    private static Trie<Node> fqdnTagFuzzyMatchRules = new Trie<>();
    private static Map<String, Node> fqdnTagFullMatchRules = new HashMap<>();
    private static Map<String, Node> appLabelTagRules = new HashMap<>();

    private static List<String> ipInvalid = new ArrayList<>();
    private static List<String> domainInvalid = new ArrayList<>();
    private static List<String> appInvalid = new ArrayList<>();

    public static void updateInvalid(List<String> list1, List<String> list2, List<String> list3) {
        ipInvalid = list1;
        domainInvalid = list2;
        appInvalid = list3;
    }

    public static void updateIpTag(Map<Long, byte[]> contents) {
        TreeRangeMap<IPAddress, Node> newIpTagRules = TreeRangeMap.create();
        for (Long s : contents.keySet()) {
            parseIpTagRules(newIpTagRules, s, contents.get(s));
        }
        ipTagRules = newIpTagRules;
    }

    public static void updateDomainTag(Map<Long, byte[]> contents) {
        Trie<Node> newFqdnTagFuzzyMatchRules = new Trie<>();
        Map<String, Node> newFqdnTagFullMatchRules = new HashMap<>();
        for (Long s : contents.keySet()) {
            parseFqdnTagRules(newFqdnTagFuzzyMatchRules, newFqdnTagFullMatchRules, s, contents.get(s));
        }
        fqdnTagFuzzyMatchRules = newFqdnTagFuzzyMatchRules;
        fqdnTagFullMatchRules = newFqdnTagFullMatchRules;
    }

    public static void updateAppTag(Map<Long, byte[]> contents) {
        Map<String, Node> newAppLabelTagRules = new HashMap<>();
        for (Long s : contents.keySet()) {
            parseAppLabelTagRules(newAppLabelTagRules, s, contents.get(s));
        }
        appLabelTagRules = newAppLabelTagRules;
    }

    private static void parseIpTagRules(TreeRangeMap<IPAddress, Node> treeRangeMap, Long id, byte[] ipFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(ipFileContent));
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            HighCsvReader csvReader = new HighCsvReader(reader, needColumns);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
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

                    RangeMap<IPAddress, Node> ipAddressListRangeMap = treeRangeMap.subRangeMap(Range.closed(startIpAddress, endIpAddress));
                    Map<Range<IPAddress>, Node> rangeListMap = ipAddressListRangeMap.asMapOfRanges();
                    TreeRangeMap<IPAddress, Node> subRangeMap = TreeRangeMap.create();
                    Node node = new Node();
                    node.getTags().add(tagValue);
                    node.getKnowledgeIds().add(id);
                    node.getKnowledgeTypes().add("ip");
                    subRangeMap.put(Range.closed(startIpAddress, endIpAddress), node);
                    for (Range<IPAddress> ipAddressRange : rangeListMap.keySet()) {
                        Node ipAddressNode = ipAddressListRangeMap.get(ipAddressRange.lowerEndpoint());
                        ipAddressNode.getTags().add(tagValue);
                        node.getKnowledgeIds().add(id);
                        node.getKnowledgeTypes().add("ip");
                        subRangeMap.put(ipAddressRange, ipAddressNode);
                    }
                    treeRangeMap.putAll(subRangeMap);
                } catch (Exception lineException) {
                    System.out.println("ip tag line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
        } catch (Exception e) {
            logger.error("parse ip rules id" + id + " failed", e);
        }


    }

    private static void parseFqdnTagRules(Trie<Node> fqdnTagsFuzzy, Map<String, Node> fqdnTagsFull, Long id, byte[] fqdnFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(fqdnFileContent));
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("domain");
            HighCsvReader csvReader = new HighCsvReader(reader, needColumns);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
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
                    Node node = new Node();
                    node.getTags().add(tagValue);
                    node.getKnowledgeIds().add(id);
                    node.getKnowledgeTypes().add("domain");
                    fqdnTagsFuzzy.put(StringUtils.reverse(newRule), node);
                } else {
                    if (fqdnTagsFull.containsKey(newRule)) {
                        Node node = fqdnTagsFull.get(newRule);
                        node.getTags().add(tagValue);
                        node.getKnowledgeIds().add(id);
                        node.getKnowledgeTypes().add("domain");
                    } else {
                        Node node = new Node();
                        node.getTags().add(tagValue);
                        node.getKnowledgeIds().add(id);
                        node.getKnowledgeTypes().add("domain");
                        fqdnTagsFull.put(newRule, node);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("parse fqdn rules id" + id + " failed", e);
        }


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
                    node.getKnowledgeIds().add(id);
                    node.getKnowledgeTypes().add("app");
                } else {
                    Node node = new Node();
                    node.getTags().add(tagValue);
                    node.getKnowledgeIds().add(id);
                    node.getKnowledgeTypes().add("app");
                    appTags.put(appName, node);
                }
            }
        } catch (Exception e) {
            logger.error("parse app rules id" + id + " failed", e);
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
        Node clientIpNode = getIpNode(entity.getCommon_client_ip());
        entity.setClient_ip_tags(clientIpNode.getTags());
        //client ip not need detection
        /*entity.setRule_id_list(clientIpNode.getKnowledgeIds());
        entity.setIoc_type_list(clientIpNode.getKnowledgeTypes());*/
        Node serverIpNode = getIpNode(entity.getCommon_server_ip());
        entity.setServer_ip_tags(serverIpNode.getTags());
        entity.setRule_id_list(serverIpNode.getKnowledgeIds());
        entity.setIoc_type_list(serverIpNode.getKnowledgeTypes());
    }

    private static Node getIpNode(String ip) {
        Node node = null;
        IPAddress address = new IPAddressString(ip).getAddress();
        if (address != null) {
            node = ipTagRules.get(address);
            }
        return node == null ? new Node() : node;
    }

    private static void setFqdnTags(CnRecordLog entity) {
        Node domainNode = getFqdnTags(entity.getDomain());
        entity.setDomain_tags(domainNode.getTags());
        entity.setRule_id_list(domainNode.getKnowledgeIds());
        entity.setIoc_type_list(domainNode.getKnowledgeTypes());
    }

    private static Node getFqdnTags(String fqdn) {
        Node node = new Node();
        if (fqdn == null || fqdn.length() == 0) {
            return node;
        }

        List<Node> nodes = fqdnTagFuzzyMatchRules.get(StringUtils.reverse(fqdn));
        node.mergeAll(nodes);

        if (fqdnTagFullMatchRules.containsKey(fqdn)) {
            Node currentNode = fqdnTagFullMatchRules.get(fqdn);
            node.merge(currentNode);
        }

        return node;
    }

    private static void setAppLabelTags(CnRecordLog entity) {
        Node appLabelNode = getAppLabelNode(entity.getCommon_app_label());
        entity.setApp_tags(appLabelNode.getTags());
        entity.setRule_id_list(appLabelNode.getKnowledgeIds());
        entity.setIoc_type_list(appLabelNode.getKnowledgeTypes());
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

    private static boolean isMatch(String rule, String context) {
        return context != null && context.endsWith(rule);
    }

    private static class Node {
        private List<String> tags = new ArrayList<>();
        private List<Long> knowledgeIds = new ArrayList<>();

        private List<String> knowledgeTypes = new ArrayList<>();

        public List<String> getTags() {
            return tags;
        }

        public List<Long> getKnowledgeIds() {
            return knowledgeIds;
        }

        public List<String> getKnowledgeTypes() {
            return knowledgeTypes;
        }

        public void merge(Node node) {
            this.tags.addAll(node.getTags());
            this.knowledgeIds.addAll(node.getKnowledgeIds());
            this.knowledgeTypes.addAll(node.getKnowledgeTypes());
        }

        public void mergeAll(List<Node> nodes) {
            for (Node node : nodes) {
                this.merge(node);
            }
        }
    }
}
