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

public class DomainKnowledge extends AbstractKnowledge {

    private static final Logger logger = LoggerFactory.getLogger(DomainKnowledge.class);

    private Trie<Node> fqdnTagFuzzyMatchRules = new Trie<>();
    private Map<String, Node> fqdnTagFullMatchRules = new HashMap<>();

    @Override
    public CustomKnowledgeUtils.LibraryType getType() {
        return CustomKnowledgeUtils.LibraryType.DOMAIN;
    }

    @Override
    public void parseContents(Map<String, byte[]> contents) {
        Trie<Node> newFqdnTagFuzzyMatchRules = new Trie<>();
        Map<String, Node> newFqdnTagFullMatchRules = new HashMap<>();
        for (String s : contents.keySet()) {
            parseFqdnTagRules(newFqdnTagFuzzyMatchRules, newFqdnTagFullMatchRules, convertId(s), contents.get(s));
        }
        fqdnTagFuzzyMatchRules = newFqdnTagFuzzyMatchRules;
        fqdnTagFullMatchRules = newFqdnTagFullMatchRules;
    }

    @Override
    public void setTag(CnRecordLog entity) {
        try {
            Node domainNode = getFqdnTags(entity.getDomain());
            entity.putDomain_tags(domainNode.getTags());
            CustomKnowledgeUtils.setRuleIdAndIocType(entity, domainNode);
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private Node getFqdnTags(String fqdn) {
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

    private void parseFqdnTagRules(Trie<Node> fqdnTagsFuzzy, Map<String, Node> fqdnTagsFull, Long id, byte[] fqdnFileContent) {
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
                    node.add(tagValue, id);
                    fqdnTagsFuzzy.put(StringUtils.reverse(newRule), node);
                } else {
                    if (fqdnTagsFull.containsKey(newRule)) {
                        Node node = fqdnTagsFull.get(newRule);
                        node.add(tagValue, id);
                    } else {
                        Node node = new Node();
                        node.add(tagValue, id);
                        fqdnTagsFull.put(newRule, node);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("parse fqdn rules id" + id + " failed", e);
        }
    }
}
