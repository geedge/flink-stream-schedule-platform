package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.DnsServerInfo;
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
    private static final Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static HashMap<String, DnsServerInfo> dnsMap = new HashMap<>();

    public static void readServerCsv(Reader serverReader) {

        try {
            CsvReaderUtils csvReader = new CsvReaderUtils(serverReader);
            List<Map<String, String>> data = csvReader.readAll();
            HashMap<String, DnsServerInfo> newDnsMap = new HashMap<>(data.size() * 2);
            for (Map<String, String> line : data) {
                try {
                    String ip = line.get("ip_addr");
                    String dnsServerRole = line.get("dns_server_role");
                    String orgnization = line.get("orgnization");
                    String operationSystem = line.get("operation_system");
                    String dnsSoftware = line.get("dns_software");
                    String dnssecSupportStr = line.get("dnssec_support");
                    int dnssecSupport = Integer.parseInt("".equals(dnssecSupportStr) ? "0" : dnssecSupportStr);
                    String dotSupportStr = line.get("dot_support");
                    int dotSupport = Integer.parseInt("".equals(dotSupportStr) ? "0" : dotSupportStr);
                    String dohSupportStr = line.get("doh_support");
                    int dohSupport = Integer.parseInt("".equals(dohSupportStr) ? "0" : dohSupportStr);
                    String rootMirrorLocations = line.get("root_mirror_locations");

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

                    newDnsMap.put(ip, catalog);
                } catch (Exception lineException) {
                    System.out.println("DnsServerUtils line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            dnsMap = newDnsMap;
        } catch (Exception e) {
            LOG.error("dns server init error", e);
        }
    }

    public static void setWebSktMessage(CnRecordLog cnRecordLog) {
        try {
            String commonServerIp = cnRecordLog.getCommon_server_ip();
            ArrayList<String> dnsServerRoles = new ArrayList<>();
            if (cnRecordLog.getDns_aa() == 1) {
                dnsServerRoles.add("ADNS");
            }
            if (dnsMap.containsKey(commonServerIp)) {
                DnsServerInfo dnsServerInfo = dnsMap.get(commonServerIp);
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

