package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class WhoisUtilsTest {

    @Before
    public void setUp() throws Exception {
        String path = this.getClass().getResource("/XJ_top.json").getPath();
        System.out.println("path:" + path);
        WhoisUtils.readJson(path);
    }

    @Test
    public void setWhoisSketchMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setDomain("baidu.com");

        WhoisUtils.setWhoisSketchMessage(cnRecordLog);

        assertEquals("beijing baidu netcom science technology co., ltd.",cnRecordLog.getDomain_whois_org());

    }
}