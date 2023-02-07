package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class IPUtilsTest {

    @Before
    public void setUp() throws Exception {
        String path = this.getClass().getResource("/xjlt_idc_internal_ip_20220422.csv").getPath();
        System.out.println("path:" + path);
//        IPUtils.readInternalIpCsv(path);
    }

    @Test
    public void setInternalIpFlag() {
        //todo
        /*CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_client_ip("192.168.40.55");
        cnRecordLog.setCommon_server_ip("203.207.200.1");
        cnRecordLog.setCommon_s2c_byte_num(1);
        cnRecordLog.setCommon_s2c_pkt_num(2);
        cnRecordLog.setCommon_c2s_byte_num(3);
        cnRecordLog.setCommon_c2s_pkt_num(4);
        cnRecordLog.setCommon_sessions(5);

        IPUtils.setInternalIpFlag(cnRecordLog);

        assertEquals("internal",cnRecordLog.getClient_zone());
        assertEquals("external",cnRecordLog.getServer_zone());
        assertEquals(1,cnRecordLog.getTraffic_inbound_byte());
        assertEquals(2,cnRecordLog.getTraffic_inbound_pkt());
        assertEquals(5,cnRecordLog.getInternal_query_num());*/
    }
}