package com.zdjizhi.etl.common;

public class DomainCategoryReputation {
    private String fqdn;
    private String categoryName;
    private String categoryGroup;
    private String reputationLevel;
    private long reputationScore;

    public String getFqdn() {
        return fqdn;
    }

    public void setFqdn(String fqdn) {
        this.fqdn = fqdn;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public String getCategoryGroup() {
        return categoryGroup;
    }

    public void setCategoryGroup(String categoryGroup) {
        this.categoryGroup = categoryGroup;
    }

    public String getReputationLevel() {
        return reputationLevel;
    }

    public void setReputationLevel(String reputationLevel) {
        this.reputationLevel = reputationLevel;
    }

    public long getReputationScore() {
        return reputationScore;
    }

    public void setReputationScore(long reputationScore) {
        this.reputationScore = reputationScore;
    }
}
