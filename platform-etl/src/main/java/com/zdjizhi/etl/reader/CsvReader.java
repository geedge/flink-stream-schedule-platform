package com.zdjizhi.etl.reader;

import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.utils.*;

import java.io.*;
import java.util.Map;

public class CsvReader {

    private final String fileSuffix = ".csv";

    public void loadCsv() {
        AppUtils.readCsv(getReader(CommonInternalConfig.APPSKT_TYPE + fileSuffix));
        WhoisUtils.readCsv(getReader(CommonInternalConfig.WHOIS_TYPE + fileSuffix));
        IdcRenterUtils.readServerCsv(getReader(CommonInternalConfig.IDC_RENTER_TYPE + fileSuffix));
        LinkUtils.readLinkCsv(getReader(CommonInternalConfig.LINK_TYPE + fileSuffix));
        FcUtils.readCsv(getReader(CommonInternalConfig.WEBSKT_TYPE + fileSuffix));
        FcUtils.readIcpCsv(getReader(CommonInternalConfig.ICP_TYPE + fileSuffix));
        DnsServerUtils.readServerCsv(getReader(CommonInternalConfig.DNS_TYPE + fileSuffix));
        IPUtils.readInternalIpCsv(getReader(CommonInternalConfig.INTERNAL_IP_TYPE + fileSuffix));
    }

    public void updateCsv(Map<String, byte[]> knowledgeFileCache) {
        for (Map.Entry<String, byte[]> entry : knowledgeFileCache.entrySet()) {
            if ((CommonInternalConfig.APPSKT_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                AppUtils.readCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.WHOIS_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                WhoisUtils.readCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.WEBSKT_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                FcUtils.readCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.DNS_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                DnsServerUtils.readServerCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.ICP_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                FcUtils.readIcpCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.LINK_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                LinkUtils.readLinkCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.IDC_RENTER_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                IdcRenterUtils.readServerCsv(getReader(entry.getValue()));
            }
            if ((CommonInternalConfig.INTERNAL_IP_TYPE + fileSuffix).equals(entry.getKey()) && entry.getValue() != null) {
                IPUtils.readInternalIpCsv(getReader(entry.getValue()));
            }
        }
    }

    private InputStreamReader getReader(byte[] bytes) {
        return new InputStreamReader(new ByteArrayInputStream(bytes));
    }

    private InputStreamReader getReader(String fileRelativePath) {
        byte[] bytes = FileUtils.getFileBytes(fileRelativePath);
        return getReader(bytes);
    }


}
