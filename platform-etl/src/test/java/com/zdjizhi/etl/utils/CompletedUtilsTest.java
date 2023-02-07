package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.junit.Test;

import static org.junit.Assert.*;

public class CompletedUtilsTest {

    @Test
    public void setCommonRecvTime() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_end_time(1655302496);

        CompletedUtils.setCompletedMessage(cnRecordLog);

        assertEquals(1655302496,cnRecordLog.getCommon_recv_time());

    }

    @Test
    public void setHttpStatusCode() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setHttp_response_line("test 200");

        CompletedUtils.setCompletedMessage(cnRecordLog);

        assertEquals(200,cnRecordLog.getHttp_status_code());
    }

    @Test
    public void setNotCommonMetric() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        cnRecordLog.setCommon_l4_protocol("IPv4_UDP");

        CompletedUtils.setCompletedMessage(cnRecordLog);

        assertEquals(200,cnRecordLog.getHttp_status_code());
    }

}