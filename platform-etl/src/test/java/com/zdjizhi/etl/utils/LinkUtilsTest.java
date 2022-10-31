package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LinkUtilsTest {

    @Before
    public void setUp() throws Exception {
        String path = this.getClass().getResource("/link_direction.csv").getPath();
        System.out.println("path:" + path);
        LinkUtils.readLinkCsv(path);
    }

    @Test
    public void setLinkMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_egress_link_id(256);
        cnRecordLog.setCommon_ingress_link_id(513);

        LinkUtils.setLinkMessage(cnRecordLog);

        assertEquals("西安",cnRecordLog.getEgress_link_direction());
        assertEquals("太原",cnRecordLog.getIngress_link_direction());

    }
}