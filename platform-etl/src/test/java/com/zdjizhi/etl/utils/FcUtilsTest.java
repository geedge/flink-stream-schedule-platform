package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.KnowledgeConstant;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class FcUtilsTest {

    @Before
    public void setUp() throws Exception {

        String websktPath = this.getClass().getResource("/websktDB_full-20211123-101740.csv").getPath();
        String icpPath = this.getClass().getResource("/icp.csv").getPath();
        System.out.println("websktPath:" + websktPath);
        System.out.println("icpPath:" + icpPath);
        FcUtils.readCsv(websktPath);
        FcUtils.readIcpCsv(icpPath);
    }

    @Test
    public void setWebSketchMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setDomain("baidu.com");

        FcUtils.setWebSketchMessage(cnRecordLog);

        assertEquals(81,cnRecordLog.getDomain_reputation_score());
        assertEquals("Productivity",cnRecordLog.getDomain_category_group());
        assertEquals("Search Engines",cnRecordLog.getDomain_category_name());
        assertEquals("Trustworthy",cnRecordLog.getDomain_reputation_level());
    }

    @Test
    public void setIcpMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setDomain("baidu.com");

        FcUtils.setIcpMessage(cnRecordLog);

        assertEquals("北京百度网讯科技有限公司",cnRecordLog.getDomain_icp_company_name());
    }
}