package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class DnsServerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static HashMap<String, List<String>> dnsMap = new HashMap<>();

    public static void readServerCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, List<String>> newDnsMap = new HashMap<>(csvReader.getLineNumber() * 2);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String ip = line.get("ip_addr");
                    String dnsServerRole = line.get("dns_server_role");
                    if (newDnsMap.containsKey(ip)) {
                        newDnsMap.get(ip).add(dnsServerRole);
                    } else {
                        List<String> tags = new ArrayList<>();
                        tags.add(dnsServerRole);
                        newDnsMap.put(ip, tags);
                    }
                } catch (Exception lineException) {
                    LOG.error("DnsServerUtils line: " + line.toString() + " parse error：" + lineException, lineException);
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
            if (dnsMap.containsKey(commonServerIp)) {
                cnRecordLog.putServer_ip_tags(dnsMap.get(commonServerIp));
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }

    public static void close() {
        dnsMap.clear();
        dnsMap = null;
    }
}

