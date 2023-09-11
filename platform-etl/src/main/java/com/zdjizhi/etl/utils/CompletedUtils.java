package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.CommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/8
 * @ Description
 */
public class CompletedUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedUtils.class);

    private static final List<String> L7_PROTOCOLS = Arrays.stream(CommonConfig.L7_PROTOCOL.split(",")).collect(Collectors.toList());

    public static void setCompletedMessage(CnRecordLog recordLog){
        try {
            String appFullPath = recordLog.getCommon_app_full_path();
            String l7Protocol = "UNCATEGORIZED";
            String[] split = appFullPath.split("\\.");
            int i = 0;
            while (i < split.length && L7_PROTOCOLS.contains(split[i])) {
                l7Protocol = split[i];
                i++;
            }
            recordLog.setCommon_l7_protocol(l7Protocol);
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

