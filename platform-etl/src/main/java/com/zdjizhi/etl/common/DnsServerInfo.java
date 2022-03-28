package com.zdjizhi.etl.common;

public class DnsServerInfo {

    private String ipAddr;
    private String locationCountry;
    private String locationProvince;
    private String locationRegion;
    private String asn;
    private String dnsServerRole;
    private String orgnization;
    private String operationSystem;
    private String dnsSoftware;
    private int dnssecSupport;
    private int dotSupport;
    private int dohSupport;
    private String rootMirrorLocations;

    public String getIpAddr() {
        return ipAddr;
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public String getLocationCountry() {
        return locationCountry;
    }

    public void setLocationCountry(String locationCountry) {
        this.locationCountry = locationCountry;
    }

    public String getLocationProvince() {
        return locationProvince;
    }

    public void setLocationProvince(String locationProvince) {
        this.locationProvince = locationProvince;
    }

    public String getLocationRegion() {
        return locationRegion;
    }

    public void setLocationRegion(String locationRegion) {
        this.locationRegion = locationRegion;
    }

    public String getAsn() {
        return asn;
    }

    public void setAsn(String asn) {
        this.asn = asn;
    }

    public String getDnsServerRole() {
        return dnsServerRole;
    }

    public void setDnsServerRole(String dnsServerRole) {
        this.dnsServerRole = dnsServerRole;
    }

    public String getOrgnization() {
        return orgnization;
    }

    public void setOrgnization(String orgnization) {
        this.orgnization = orgnization;
    }

    public String getOperationSystem() {
        return operationSystem;
    }

    public void setOperationSystem(String operationSystem) {
        this.operationSystem = operationSystem;
    }

    public String getDnsSoftware() {
        return dnsSoftware;
    }

    public void setDnsSoftware(String dnsSoftware) {
        this.dnsSoftware = dnsSoftware;
    }

    public int getDnssecSupport() {
        return dnssecSupport;
    }

    public void setDnssecSupport(int dnssecSupport) {
        this.dnssecSupport = dnssecSupport;
    }

    public int getDotSupport() {
        return dotSupport;
    }

    public void setDotSupport(int dotSupport) {
        this.dotSupport = dotSupport;
    }

    public int getDohSupport() {
        return dohSupport;
    }

    public void setDohSupport(int dohSupport) {
        this.dohSupport = dohSupport;
    }

    public String getRootMirrorLocations() {
        return rootMirrorLocations;
    }

    public void setRootMirrorLocations(String rootMirrorLocations) {
        this.rootMirrorLocations = rootMirrorLocations;
    }
}
