package com.zdjizhi.etl.common;

public class AppSketchLabel {
    private String appName;
    private String category;
    private String subCategory;
    private String appCompany;

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

    @Override
    public String toString() {
        return "AppSketchLabel{" +
                "appName='" + appName + '\'' +
                ", category='" + category + '\'' +
                ", appCompany='" + appCompany + '\'' +
                '}';
    }
}
