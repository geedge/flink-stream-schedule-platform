package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.etl.common.DnsServerInfo;
import com.zdjizhi.etl.common.LinkAndIspInfo;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class DnsServerUtils {
    private static Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static HashMap<String, DnsServerInfo> dnsmap = new HashMap<>();

    public static void readServerCsv(Reader serverReader) {

        try {
            CSVReader csvReader = new CSVReader(serverReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<String, DnsServerInfo> newDnsmap = new HashMap<>(lines.size() * 2);
            for (String[] line : lines) {
                try {
                    String ip = line[1];
                    String dnsServerRole = line[6];
                    String orgnization = line[7];
                    String operationSystem = line[8];
                    String dnsSoftware = line[9];
                    int dnssecSupport = Integer.parseInt("".equals(line[10]) ? "0" : line[10]);
                    int dotSupport = Integer.parseInt("".equals(line[11]) ? "0" : line[11]);
                    int dohSupport = Integer.parseInt("".equals(line[12]) ? "0" : line[12]);
                    String rootMirrorLocations = line[13];

                    DnsServerInfo catalog = new DnsServerInfo();
                    catalog.setIpAddr(ip);
                    catalog.setDnsServerRole(dnsServerRole);
                    catalog.setOrgnization(orgnization);
                    catalog.setOperationSystem(operationSystem);
                    catalog.setDnsSoftware(dnsSoftware);
                    catalog.setDnssecSupport(dnssecSupport);
                    catalog.setDotSupport(dotSupport);
                    catalog.setDohSupport(dohSupport);
                    catalog.setRootMirrorLocations(rootMirrorLocations);

                    newDnsmap.put(ip, catalog);
                } catch (Exception lineException) {
                    System.out.println("DnsServerUtils line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            dnsmap = newDnsmap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error("加载失败", e);
        }
    }

    public static void setWebSktMessage(CnRecordLog cnRecordLog) {
        try {
            String commonServerIp = cnRecordLog.getCommon_server_ip();
            ArrayList<String> dnsServerRoles = new ArrayList<>();
            if (cnRecordLog.getDns_aa() == 1) {
                dnsServerRoles.add("ADNS");
            }
            if (dnsmap.containsKey(commonServerIp)) {
                DnsServerInfo dnsServerInfo = dnsmap.get(commonServerIp);
                String dnsServerRole = dnsServerInfo.getDnsServerRole();
                dnsServerRoles.add(dnsServerRole);
                cnRecordLog.setDns_server_org(dnsServerInfo.getOrgnization());
                cnRecordLog.setDns_server_os(dnsServerInfo.getOperationSystem());
                cnRecordLog.setDns_server_software(dnsServerInfo.getDnsSoftware());
                if (cnRecordLog.getCommon_server_port() == 443 && dnsServerInfo.getDohSupport() == 1) {
                    cnRecordLog.setDns_protocol("doh");
                } else if (cnRecordLog.getCommon_server_port() == 853 && dnsServerInfo.getDotSupport() == 1) {
                    cnRecordLog.setDns_protocol("dot");
                } else if (cnRecordLog.getDns_sub() == 2) {
                    cnRecordLog.setDns_protocol("dnssec");
                }
            }
            cnRecordLog.setDns_server_role(dnsServerRoles);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }
}

