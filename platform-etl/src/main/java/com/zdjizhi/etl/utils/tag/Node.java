package com.zdjizhi.etl.utils.tag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Node {
    private final List<String> tags = new ArrayList<>();
    protected Set<Long> knowledgeIds = new HashSet<>();

    public List<String> getTags() {
        return tags;
    }

    public void add(String tag, Long id) {
        if (tag == null) {
            return;
        }
        tags.add(tag);
        knowledgeIds.add(id);
    }

    public void merge(Node node) {
        this.tags.addAll(node.getTags());
        this.knowledgeIds.addAll(node.knowledgeIds);
    }

    public void mergeAll(List<Node> nodes) {
        for (Node node : nodes) {
            this.merge(node);
        }
    }
}
