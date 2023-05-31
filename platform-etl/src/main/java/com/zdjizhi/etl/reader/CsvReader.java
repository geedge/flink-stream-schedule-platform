package com.zdjizhi.etl.reader;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.utils.*;

import java.io.*;
import java.util.Map;

public class CsvReader {

    private final String fileSuffix = ".csv";

    public void loadCsv() {
        AppUtils.readCsv(getReader(CommonConfig.APPSKT_FILENAME + fileSuffix));
        WhoisUtils.readCsv(getReader(CommonConfig.WHOIS_FILENAME + fileSuffix));
        IdcRenterUtils.readServerCsv(getReader(CommonConfig.IDC_RENTER_FILENAME + fileSuffix));
        LinkUtils.readLinkCsv(getReader(CommonConfig.LINK_FILENAME + fileSuffix));
        FcUtils.readCsv(getReader(CommonConfig.WEBSKT_FILENAME + fileSuffix));
        FcUtils.readIcpCsv(getReader(CommonConfig.ICP_FILENAME + fileSuffix));
        DnsServerUtils.readServerCsv(getReader(CommonConfig.DNS_FILENAME + fileSuffix));
        IPUtils.readInternalIpCsv(getReader(CommonConfig.INTERNAL_IP_FILENAME + fileSuffix));
    }

    public void updateCsv(Map<String, byte[]> knowledgeFileCache) {
        for (Map.Entry<String, byte[]> entry : knowledgeFileCache.entrySet()) {
            if ((CommonConfig.APPSKT_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                AppUtils.readCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.WHOIS_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                WhoisUtils.readCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.WEBSKT_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                FcUtils.readCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.DNS_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                DnsServerUtils.readServerCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.ICP_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                FcUtils.readIcpCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.LINK_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                LinkUtils.readLinkCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.IDC_RENTER_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                IdcRenterUtils.readServerCsv(getReader(entry.getValue()));
            }
            if ((CommonConfig.INTERNAL_IP_FILENAME + fileSuffix).equals(entry.getKey()) && entry.getValue() !=null) {
                IPUtils.readInternalIpCsv(getReader(entry.getValue()));
            }
        }
    }

    private InputStreamReader getReader(byte[] bytes) {
        return new InputStreamReader(new ByteArrayInputStream(bytes));
    }

    private InputStreamReader getReader(String fileRelativePath) {
        try {
            byte[] bytes = FileUtils.getFileBytes(fileRelativePath);
            return getReader(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
