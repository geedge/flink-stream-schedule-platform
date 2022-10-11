package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.utils.FormatUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/8
 * @ Description
 */
public class CompletedUtils {

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    public static void setCompletedMessage(CnRecordLog recordLog){
        recordLog.setCommon_recv_time(recordLog.getCommon_end_time());
        recordLog.setHttp_status_code(getHttpStatusCode(recordLog));

        //domain
        if (StringUtil.isNotEmpty(recordLog.getHttp_host())){
            recordLog.setDomain(FormatUtils.getTopPrivateDomain(recordLog.getHttp_host()));
        }else {
            recordLog.setDomain(FormatUtils.getTopPrivateDomain(recordLog.getSsl_sni()));
        }

//        if (recordLog.getCommon_ingress_link_id() > 2050 || recordLog.getCommon_egress_link_id() > 2050) {
//            recordLog.setCommon_ingress_link_id(0);
//            recordLog.setCommon_egress_link_id(0);
//        }

        setNotCommonMetric(recordLog);
    }

    private static Integer getHttpStatusCode(CnRecordLog recordLog){
        int httpStatusCode = 0;
        String httpResponseLine = recordLog.getHttp_response_line();
        if (StringUtil.isNotBlank(httpResponseLine)){
            String[] split = httpResponseLine.split(" ");
            if (split.length >= 2){
                httpStatusCode = Integer.parseInt(split[1]);
            }
        }
        return httpStatusCode;
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
        String commonL7Protocol = recordLog.getCommon_l7_protocol();
        if (StringUtil.isNotBlank(commonL7Protocol) && commonL7Protocol.contains("HTTP")){
            recordLog.setHttp_count(1);
        }else if ("SSL".equals(commonL7Protocol)){
            recordLog.setSsl_count(1);
        }

        if ("DNS".equals(recordLog.getCommon_schema_type())){
            recordLog.setDns_count(1);
        }

    }

    public static void main(String[] args) {
        System.out.println(FormatUtils.getTopPrivateDomain("/neuromancer"));
    }

}
