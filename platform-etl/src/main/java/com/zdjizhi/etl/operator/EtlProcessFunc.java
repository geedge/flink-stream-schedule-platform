package com.zdjizhi.etl.operator;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.CommonConfig;
import com.zdjizhi.etl.knowledge.NacosManager;
import com.zdjizhi.etl.rule.RuleUpdateListener;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.etl.utils.fqdn.CategoryUtils;
import com.zdjizhi.etl.utils.fqdn.IcpUtils;
import com.zdjizhi.etl.utils.fqdn.WhoisUtils;
import com.zdjizhi.etl.utils.ip.IPUtils;
import com.zdjizhi.etl.utils.ip.InternalIpUtils;
import com.zdjizhi.etl.utils.tag.CustomKnowledgeManager;
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
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        InternalIpUtils.init(configuration.get(CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN),
                configuration.get(CommonConfig.IP_GEO_PATTERN_FIELD),
                configuration.get(CommonConfig.IP_GEO_PATTERN_VALUE));
        CompletedUtils.init(configuration.get(CommonConfig.L7_PROTOCOL));
        NacosManager nacosManager = new NacosManager(configuration);
        nacosManager.startAsyncListen();
        RuleUpdateListener ruleUpdateListener = new RuleUpdateListener(configuration);
        ruleUpdateListener.registerListener();
    }

    @Override
    public void processElement(String value, ProcessFunction<String, CnRecordLog>.Context ctx, Collector<CnRecordLog> out) throws Exception {
        CnRecordLog recordLog = new CnRecordLog();
        try {
            recordLog = JSON.parseObject(value, CnRecordLog.class);
            if (recordLog.getCommon_recv_time() == 0) {
                if (ctx.timestamp() != 0) {
                    recordLog.setCommon_recv_time(ctx.timestamp() / 1000);
                } else {
                    recordLog.setCommon_recv_time(System.currentTimeMillis() / 1000);
                }
            }
            CompletedUtils.setCompletedMessage(recordLog);
            IPUtils.setMmdbMessage(recordLog);
            IdcRenterUtils.setIdcRenter(recordLog);
            LinkUtils.setLinkMessage(recordLog);
            CategoryUtils.setCategoryMessage(recordLog);
            IcpUtils.setIcpMessage(recordLog);
            WhoisUtils.setWhoisSketchMessage(recordLog);
            DnsServerUtils.setWebSktMessage(recordLog);
            AppUtils.setAppSketchMessage(recordLog);
            InternalIpUtils.setInternalIpFlag(recordLog);
            PsiphonUtils.setPsiphonInfo(recordLog);
            IocDarkWebUtils.setIocDarkWebInfo(recordLog);
            IocMalwareUtils.setIocMalwareInfo(recordLog);
            CustomKnowledgeManager.setTag(recordLog);
            out.collect(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nsource:{} \nrecordLog:{} \n", value, recordLog, e);
        }
    }
}
