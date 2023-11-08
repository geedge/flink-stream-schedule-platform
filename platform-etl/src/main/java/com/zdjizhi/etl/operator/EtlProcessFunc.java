package com.zdjizhi.etl.operator;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.etl.common.CommonConfig;
import com.zdjizhi.etl.knowledge.NacosManager;
import com.zdjizhi.etl.rule.RuleUpdateListener;
import com.zdjizhi.etl.utils.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class EtlProcessFunc extends ProcessFunction<String, CnRecordLog> {

    private static final Logger LOG = LoggerFactory.getLogger(EtlProcessFunc.class);

    private int internalPolicy = 1;

    private String geoPolicyField = "province";
    private String geoPolicyValue = "";
    private String l7Protocols = "DHCP,DNS,FTP,GRE,GTP,HTTP,HTTPS,ICMP,IMAP,IMAPS,IPSEC,ISAKMP,XMPP,L2TP,LDAP,MMS,NETBIOS,NETFLOW,NTP,POP3,POP3S,RDP,PPTP,RADIUS,RTCP,RTP,RTSP,SIP,SMB,SMTP,SMTPS,SNMP,SSDP,SSH,SSL,STUN,TELNET,TFTP,OPENVPN,RTMP,TEREDO,FTPS,DTLS,SPDY,BJNP,QUIC,MDNS,Unknown TCP,Unknown UDP,Unknown Other,IKE,MAIL,SOCKS,DoH,SLP,SSL with ESNI,ISATAP,Stratum,SSL with ECH";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        internalPolicy = configuration.get(CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN);
        geoPolicyField = configuration.get(CommonConfig.IP_GEO_PATTERN_FIELD);
        geoPolicyValue = configuration.get(CommonConfig.IP_GEO_PATTERN_VALUE);
        l7Protocols = configuration.get(CommonConfig.L7_PROTOCOL);
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
            if (ctx.timestamp() != 0) {
                recordLog.setCommon_recv_time(ctx.timestamp() / 1000);
            } else {
                recordLog.setCommon_recv_time(System.currentTimeMillis() / 1000);
            }
            CompletedUtils.setCompletedMessage(recordLog, l7Protocols);
            IPUtils.setMmdbMessage(recordLog);
            IdcRenterUtils.setIdcRenter(recordLog);
            LinkUtils.setLinkMessage(recordLog);
            FcUtils.setWebSketchMessage(recordLog);
            FcUtils.setIcpMessage(recordLog);
            WhoisUtils.setWhoisSketchMessage(recordLog);
            DnsServerUtils.setWebSktMessage(recordLog);
            AppUtils.setAppSketchMessage(recordLog);
            IPUtils.setInternalIpFlag(recordLog, internalPolicy, geoPolicyField, geoPolicyValue);
            PsiphonUtils.setPsiphonInfo(recordLog);
            IocDarkWebUtils.setIocDarkWebInfo(recordLog);
            IocMalwareUtils.setIocMalwareInfo(recordLog);
            TagUtils.setTag(recordLog);
            out.collect(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nsource:{} \nrecordLog:{} \n", value, recordLog, e);
        }
    }
}
