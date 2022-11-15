package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DnsServerUtilsTest {

    @Before
    public void setUp() throws Exception {
        String path = this.getClass().getResource("/webskt_default_dns_server_info_full_20220427.csv").getPath();
        System.out.println("path:" + path);
//        DnsServerUtils.readServerCsv(path);
    }

    @Test
    public void setWebSktMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_server_ip("152.101.4.130");
        cnRecordLog.setDns_aa(1);
        cnRecordLog.setCommon_schema_type("DNS");
        cnRecordLog.setCommon_server_port(443);
        cnRecordLog.setDns_sub(1);

        DnsServerUtils.setWebSktMessage(cnRecordLog);

        assertArrayEquals(Arrays.asList("ADNS","OPRDNS").toArray(),cnRecordLog.getDns_server_role().toArray());
        assertEquals("",cnRecordLog.getDns_server_org());
        assertEquals("",cnRecordLog.getDns_server_os());
        assertEquals("",cnRecordLog.getDns_server_software());
        assertEquals("dnssec",cnRecordLog.getDns_protocol());
    }
}