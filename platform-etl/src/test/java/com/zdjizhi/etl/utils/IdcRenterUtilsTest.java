package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class IdcRenterUtilsTest {


    @Before
    public void setUp() {
        String path = this.getClass().getResource("/xjlt_ip_location_20220422.csv").getPath();
        System.out.println("path:" + path);
        IdcRenterUtils.readServerCsv(path);
    }

    @Test
    public void setIdcRenter() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_server_ip("203.207.200.1");//测试csv中的 203.207.200.0/24
        cnRecordLog.setCommon_client_ip("116.178.252.1");//测试csv中没有的ip

        IdcRenterUtils.setIdcRenter(cnRecordLog);

        assertEquals("乌鲁木齐",cnRecordLog.getServer_idc_renter());
        assertNull(cnRecordLog.getClient_idc_renter());
    }
}