package com.zdjizhi.etl.common;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/30
 * @ Description
 */
public class LinkAndIspInfo {
    private String egress_link_direction;
    private String ingress_link_direction;
    private String region;

    public String getEgress_link_direction() {
        return egress_link_direction;
    }

    public void setEgress_link_direction(String egress_link_direction) {
        this.egress_link_direction = egress_link_direction;
    }

    public String getIngress_link_direction() {
        return ingress_link_direction;
    }

    public void setIngress_link_direction(String ingress_link_direction) {
        this.ingress_link_direction = ingress_link_direction;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "LinkDirectionInfo{" +
                "egress_link_direction='" + egress_link_direction + '\'' +
                ", ingress_link_direction='" + ingress_link_direction + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
