package com.zdjizhi.etl.common;

public class DomainWhois {
    private String fqdn;
    private String whoisRegistrantOrg;

    public String getFqdn() {
        return fqdn;
    }

    public void setFqdn(String fqdn) {
        this.fqdn = fqdn;
    }

    public String getWhoisRegistrantOrg() {
        return whoisRegistrantOrg;
    }

    public void setWhoisRegistrantOrg(String whoisRegistrantOrg) {
        this.whoisRegistrantOrg = whoisRegistrantOrg;
    }
}
