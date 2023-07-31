package com.zdjizhi.etl.operator;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtlProcessFunc extends ProcessFunction<String, CnRecordLog> {

    private static final Logger LOG = LoggerFactory.getLogger(EtlProcessFunc.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        NacosUtils nacosUtils = new NacosUtils();
    }

    @Override
    public void processElement(String value, ProcessFunction<String, CnRecordLog>.Context ctx, Collector<CnRecordLog> out) throws Exception {
        CnRecordLog recordLog = new CnRecordLog();
        try {
            recordLog = JSON.parseObject(value, CnRecordLog.class);
            CompletedUtils.setCompletedMessage(recordLog);
            IPUtils.setMmdbMessage(recordLog);
            IdcRenterUtils.setIdcRenter(recordLog);
            LinkUtils.setLinkMessage(recordLog);
            FcUtils.setWebSketchMessage(recordLog);
            FcUtils.setIcpMessage(recordLog);
            WhoisUtils.setWhoisSketchMessage(recordLog);
            DnsServerUtils.setWebSktMessage(recordLog);
            AppUtils.setAppSketchMessage(recordLog);
            IPUtils.setInternalIpFlag(recordLog);
            TagUtils.setTag(recordLog);
            out.collect(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nsource:{} \nrecordLog:{} \n", value, recordLog, e);
        }
    }
}
