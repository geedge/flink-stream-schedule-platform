package com.zdjizhi.etl.utils.tag;

import java.util.ArrayList;
import java.util.List;

public class Node {
    private List<String> tags = new ArrayList<>();
    protected List<Long> knowledgeIds = new ArrayList<>();

    protected List<String> knowledgeTypes = new ArrayList<>();

    public List<String> getTags() {
        return tags;
    }

    public void put(Long id, String type) {
        if (id == null || type == null) {
            return;
        }
        knowledgeIds.add(id);
        knowledgeTypes.add(type);
    }

    public void merge(Node node) {
        this.tags.addAll(node.getTags());
        this.knowledgeIds.addAll(node.knowledgeIds);
        this.knowledgeTypes.addAll(node.knowledgeTypes);
    }

    public void mergeAll(List<Node> nodes) {
        for (Node node : nodes) {
            this.merge(node);
        }
    }
}
