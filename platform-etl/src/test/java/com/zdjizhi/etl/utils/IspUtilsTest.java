package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class IspUtilsTest {

    @Before
    public void setUp() {
        String path = this.getClass().getResource("/xjlt_ip_location_20220422.csv").getPath();
        System.out.println("path:" + path);
        IspUtils.readServerCsv(path);
    }

    @Test
    public void setIspMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_server_ip("203.207.200.1");
        cnRecordLog.setCommon_client_ip("116.178.252.1");

        IspUtils.setIspMessage(cnRecordLog);

        assertNull(cnRecordLog.getServer_region());
        assertEquals("乌鲁木齐",cnRecordLog.getClient_region());

    }
}