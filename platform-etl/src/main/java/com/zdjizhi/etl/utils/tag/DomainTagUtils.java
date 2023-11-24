package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.common.Trie;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DomainTagUtils {

    private static final Logger logger = LoggerFactory.getLogger(DomainTagUtils.class);

    private static Trie<Node> fqdnTagFuzzyMatchRules = new Trie<>();
    private static Map<String, Node> fqdnTagFullMatchRules = new HashMap<>();

    public static void updateTag(Map<Long, byte[]> contents) {
        Trie<Node> newFqdnTagFuzzyMatchRules = new Trie<>();
        Map<String, Node> newFqdnTagFullMatchRules = new HashMap<>();
        for (Long s : contents.keySet()) {
            parseFqdnTagRules(newFqdnTagFuzzyMatchRules, newFqdnTagFullMatchRules, s, contents.get(s));
        }
        fqdnTagFuzzyMatchRules = newFqdnTagFuzzyMatchRules;
        fqdnTagFullMatchRules = newFqdnTagFullMatchRules;
    }

    public static void setTag(CnRecordLog entity) {
        try {
            Node domainNode = getFqdnTags(entity.getDomain());
            entity.putDomain_tags(domainNode.getTags());
            BaseTagUtils.setRuleIdAndIocType(entity, domainNode);
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
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
                if (rule.startsWith("*")) {
                    Node node = new Node();
                    node.getTags().add(tagValue);
                    node.put(id, "domain");
                    fqdnTagsFuzzy.put(StringUtils.reverse(newRule), node);
                } else {
                    if (fqdnTagsFull.containsKey(newRule)) {
                        Node node = fqdnTagsFull.get(newRule);
                        node.getTags().add(tagValue);
                        node.put(id, "domain");
                    } else {
                        Node node = new Node();
                        node.getTags().add(tagValue);
                        node.put(id, "domain");
                        fqdnTagsFull.put(newRule, node);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("parse fqdn rules id" + id + " failed", e);
        }
    }
}
