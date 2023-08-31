package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/8
 * @ Description
 */
public class CompletedUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedUtils.class);

    public static void setCompletedMessage(CnRecordLog recordLog){
        try {
            recordLog.setCommon_l7_protocol(recordLog.getCommon_schema_type());
            setNotCommonMetric(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", recordLog, e);
        }
    }

    private static void setNotCommonMetric(CnRecordLog recordLog){
        String commonL4Protocol = recordLog.getCommon_l4_protocol();
        if ("IPv4_TCP".equals(commonL4Protocol) || "IPv6_TCP".equals(commonL4Protocol)){
            recordLog.setTcp_count(1);
            recordLog.setC2s_tcp_byte_num(recordLog.getCommon_c2s_byte_num());
            recordLog.setS2c_tcp_byte_num(recordLog.getCommon_s2c_byte_num());
            recordLog.setC2s_tcp_pkt_num(recordLog.getCommon_c2s_pkt_num());
            recordLog.setS2c_tcp_pkt_num(recordLog.getCommon_s2c_pkt_num());
        }
        String commonSchemaType = recordLog.getCommon_schema_type();
        if ("HTTP".equals(commonSchemaType)){
            recordLog.setHttp_count(1);
        }else if ("SSL".equals(commonSchemaType)){
            recordLog.setSsl_count(1);
        }else if ("DNS".equals(commonSchemaType)){
            recordLog.setDns_count(1);
        }

    }

}

