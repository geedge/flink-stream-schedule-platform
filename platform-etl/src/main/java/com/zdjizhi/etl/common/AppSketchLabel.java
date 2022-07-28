package com.zdjizhi.etl.common;

public class AppSketchLabel {
    private String appName;
    private String category;
    private String subCategory;
    private Long risk;
    private Integer isProtocol;
    private String appCompany;
    private Integer appId;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Long getRisk() {
        return risk;
    }

    public void setRisk(Long risk) {
        this.risk = risk;
    }

    public Integer getProtocol() {
        return isProtocol;
    }

    public void setProtocol(Integer protocol) {
        isProtocol = protocol;
    }

    public String getAppCompany() {
        return appCompany;
    }

    public void setAppCompany(String appCompany) {
        this.appCompany = appCompany;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
    }

    public Integer getIsProtocol() {
        return isProtocol;
    }

    public void setIsProtocol(Integer isProtocol) {
        this.isProtocol = isProtocol;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    @Override
    public String toString() {
        return "AppSketchLabel{" +
                "appName='" + appName + '\'' +
                ", category='" + category + '\'' +
                ", subCategory='" + subCategory + '\'' +
                ", risk=" + risk +
                ", isProtocol=" + isProtocol +
                ", appCompany='" + appCompany + '\'' +
                ", appId=" + appId +
                '}';
    }
}
