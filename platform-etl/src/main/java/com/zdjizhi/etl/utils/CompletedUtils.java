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

        String commonAppLabel = recordLog.getCommon_app_label();
        if (StringUtil.isBlank(commonAppLabel) || "unknown".equals(commonAppLabel) || commonAppLabel.startsWith("05")){
            recordLog.setCommon_app_label("");
        }

        recordLog.setHttp_status_code(getHttpStatusCode(recordLog));

        //domain
        if (StringUtil.isNotEmpty(recordLog.getHttp_host())){
            recordLog.setDomain(FormatUtils.getTopPrivateDomain(recordLog.getHttp_host()));
        }else {
            recordLog.setDomain(FormatUtils.getTopPrivateDomain(recordLog.getSsl_sni()));
        }

        if (recordLog.getCommon_ingress_link_id() > 2050 || recordLog.getCommon_egress_link_id() > 2050) {
            recordLog.setCommon_ingress_link_id(0);
            recordLog.setCommon_egress_link_id(0);
        }

        Object appId = recordLog.getCommon_app_id();
        if (appId != null){
            recordLog.setCommon_app_id(jsonMapperInstance.toJson(appId));
        }

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

    public static void main(String[] args) {
        System.out.println(FormatUtils.getTopPrivateDomain("/neuromancer"));
    }

}
