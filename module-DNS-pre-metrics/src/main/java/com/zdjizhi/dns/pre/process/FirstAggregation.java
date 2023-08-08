package com.zdjizhi.dns.pre.process;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.google.common.net.InternetDomainName;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.dns.pre.common.DnsMetricLog;
import com.zdjizhi.etl.utils.DomainUtils;
import com.zdjizhi.pre.common.MetricKeyConfig;
import com.zdjizhi.pre.operator.AbstractFirstAggregation;
import com.zdjizhi.utils.FormatUtils;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirstAggregation extends AbstractFirstAggregation<DnsMetricLog> {

    private static Logger LOG = LoggerFactory.getLogger(FirstAggregation.class);

    private final String dnsServerIpKey = "dnsServerIpMetric";
    private final String dnsQnameKey = "dnsQnameMetric";
    private final String dnsQtypeKey = "dnsQtypeMetric";
    private final String dnsRcodeKey = "dnsRcodeMetric";
    private final String dnsRraKey = "dnsRraMetric";
    private final String dnsRraaaaKey = "dnsRraaaaMetric";
    private final String dnsRrcnameKey = "dnsRrcnameMetric";

    @Override
    public Map<String, Map<String, DnsMetricLog>> createAccumulator() {
        Map<String, Map<String, DnsMetricLog>> accumulator = new HashMap<>();
        accumulator.put(dnsServerIpKey, new HashMap<>());
        accumulator.put(dnsQnameKey, new HashMap<>());
        accumulator.put(dnsQtypeKey, new HashMap<>());
        accumulator.put(dnsRcodeKey, new HashMap<>());
        accumulator.put(dnsRraKey, new HashMap<>());
        accumulator.put(dnsRraaaaKey, new HashMap<>());
        accumulator.put(dnsRrcnameKey, new HashMap<>());
        return accumulator;
    }

    @Override
    public void add(CnRecordLog element, Map<String, Map<String, DnsMetricLog>> accumulator) {
        try {
            String commonServerIp = element.getCommon_server_ip();
            Map<String, DnsMetricLog> dnsServerIpMetricMap = accumulator.get(dnsServerIpKey);
            DnsMetricLog serverIpMetric = dnsServerIpMetricMap.get(commonServerIp);
            if (serverIpMetric != null) {
                updateMetric(element, serverIpMetric);
            } else {
                serverIpMetric = new DnsMetricLog(MetricKeyConfig.DNS_SERVER_IP);
                initMetric(element, serverIpMetric);
                serverIpMetric.setCommon_server_ip(element.getCommon_server_ip());
                serverIpMetric.setServer_country(element.getServer_country());
                serverIpMetric.setServer_city(element.getServer_region());
                serverIpMetric.setServer_isp(element.getServer_isp());
                serverIpMetric.setDns_server_org(element.getDns_server_org());
                serverIpMetric.setDns_server_role(element.getDns_server_role());
            }
            dnsServerIpMetricMap.put(commonServerIp, serverIpMetric);

            try {
                if (StringUtils.isNotBlank(element.getDns_qname())) {
                    String dnsQname = element.getDns_qname();
                    String secondDomain = null;
                    String topDomain = null;
                    try {
                        //获取二级域名
                        secondDomain = DomainUtils.getTopPrivateDomain(dnsQname);
                    } catch (StringIndexOutOfBoundsException outOfBoundsException) {
                        LOG.error("Parse top-level domain exceptions, exception domain names:" + dnsQname);
                    }

//                    String secondDomain = FormatUtils.getTopPrivateDomain(dnsQname);
                    //获取公有顶级域名
                    InternetDomainName internetDomainName = InternetDomainName.from(dnsQname);
                    //判断是否有公有顶级域
                    if (internetDomainName.hasPublicSuffix()) {
                        topDomain = internetDomainName.publicSuffix().toString();
                    }
                    Map<String, DnsMetricLog> dnsQnameMetricMap = accumulator.get(dnsQnameKey);
                    DnsMetricLog dnsQnameMetric = dnsQnameMetricMap.get(dnsQname);
                    if (dnsQnameMetric != null) {
                        updateMetric(element, dnsQnameMetric);
                    } else {
                        dnsQnameMetric = new DnsMetricLog(MetricKeyConfig.DNS_QNAME);
                        initMetric(element, dnsQnameMetric);
                        dnsQnameMetric.setDns_qname(dnsQname);
                        dnsQnameMetric.setQname_sld(secondDomain);
                        dnsQnameMetric.setQname_tld(topDomain);
                    }
                    dnsQnameMetricMap.put(dnsQname, dnsQnameMetric);
                }
            } catch (IllegalArgumentException e) {
                LOG.warn("Illegal domain name" + e);
            }


            long dnsQtype = element.getDns_qtype();
            Map<String, DnsMetricLog> dnsQtypeMetricMap = accumulator.get(dnsQtypeKey);
            DnsMetricLog dnsQtypeMetric = dnsQtypeMetricMap.get(String.valueOf(dnsQtype));
            if (dnsQtypeMetric != null) {
                updateMetric(element, dnsQtypeMetric);
            } else {
                dnsQtypeMetric = new DnsMetricLog(MetricKeyConfig.DNS_QTYPE);
                initMetric(element, dnsQtypeMetric);
                dnsQtypeMetric.setDns_qtype(dnsQtype);
            }
            dnsQtypeMetricMap.put(String.valueOf(dnsQtype), dnsQtypeMetric);

            long dnsRcode = element.getDns_rcode();
            Map<String, DnsMetricLog> dnsRcodeMtricMap = accumulator.get(dnsRcodeKey);
            DnsMetricLog dnsRcodeMetric = dnsRcodeMtricMap.get(String.valueOf(dnsRcode));
            if (dnsRcodeMetric != null) {
                updateMetric(element, dnsRcodeMetric);
            } else {
                dnsRcodeMetric = new DnsMetricLog(MetricKeyConfig.DNS_RCODE);
                initMetric(element, dnsRcodeMetric);
                dnsRcodeMetric.setDns_rcode(dnsRcode);
            }
            dnsRcodeMtricMap.put(String.valueOf(dnsRcode), dnsRcodeMetric);

            String dns_rr = element.getDns_rr();
            if (StringUtil.isNotBlank(dns_rr)) {
                HashMap<String, Object> o = JSON.parseObject(dns_rr, TypeReference.mapType(HashMap.class, String.class, Object.class));
                List<HashMap<String, Object>> rr = JSON.parseArray(JSON.toJSONString(o.get("rr")), TypeReference.mapType(HashMap.class, String.class, Object.class));
                for (HashMap<String, Object> obj : rr) {
                    if (1 == Integer.parseInt(obj.get("type").toString())) {
                        if (obj.containsKey("a")) {
                            if (StringUtil.isNotBlank(obj.get("a").toString())) {
                                String rrA = obj.get("a").toString();
                                Map<String, DnsMetricLog> dnsRraMtricMap = accumulator.get(dnsRraKey);
                                DnsMetricLog dnsRraMetric = dnsRraMtricMap.get(rrA);
                                if (dnsRraMetric != null) {
                                    updateMetric(element, dnsRraMetric);
                                } else {
                                    dnsRraMetric = new DnsMetricLog(MetricKeyConfig.DNS_RRA);
                                    initMetric(element, dnsRraMetric);
                                    dnsRraMetric.setDns_rra(rrA);
                                }
                                dnsRraMtricMap.put(rrA, dnsRraMetric);
                            }
                        }
                    } else if (28 == Integer.parseInt(obj.get("type").toString())) {
                        if (obj.containsKey("aaaa")) {
                            if (StringUtil.isNotBlank(obj.get("aaaa").toString())) {
                                String rrAaaa = obj.get("aaaa").toString();
                                Map<String, DnsMetricLog> dnsRraaaaMtricMap = accumulator.get(dnsRraaaaKey);
                                DnsMetricLog dnsRraaaaMetric = dnsRraaaaMtricMap.get(rrAaaa);
                                if (dnsRraaaaMetric != null) {
                                    updateMetric(element, dnsRraaaaMetric);
                                } else {
                                    dnsRraaaaMetric = new DnsMetricLog(MetricKeyConfig.DNS_RRAAAA);
                                    initMetric(element, dnsRraaaaMetric);
                                    dnsRraaaaMetric.setDns_rraaaa(rrAaaa);
                                }
                                dnsRraaaaMtricMap.put(rrAaaa, dnsRraaaaMetric);
                            }
                        }
                    } else if (5 == Integer.parseInt(obj.get("type").toString())) {
                        if (obj.containsKey("cname")) {
                            if (StringUtil.isNotBlank(obj.get("cname").toString())) {
                                String rrCname = obj.get("cname").toString();
                                Map<String, DnsMetricLog> dnsRrcnameMtricMap = accumulator.get(dnsRrcnameKey);
                                DnsMetricLog dnsRrCnameMetric = dnsRrcnameMtricMap.get(rrCname);
                                if (dnsRrCnameMetric != null) {
                                    updateMetric(element, dnsRrCnameMetric);
                                } else {
                                    dnsRrCnameMetric = new DnsMetricLog(MetricKeyConfig.DNS_RRCNAME);
                                    initMetric(element, dnsRrCnameMetric);
                                    dnsRrCnameMetric.setDns_rrcname(rrCname);
                                }
                                dnsRrcnameMtricMap.put(rrCname, dnsRrCnameMetric);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initMetric(CnRecordLog cnRecordLog, DnsMetricLog dnsMetricLog) {
        dnsMetricLog.setCommon_start_time(cnRecordLog.getCommon_start_time());
        dnsMetricLog.setCommon_recv_time(cnRecordLog.getCommon_recv_time());
        dnsMetricLog.setCommon_sessions(cnRecordLog.getCommon_sessions());
        dnsMetricLog.setDns_count(cnRecordLog.getDns_count());
        dnsMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms());
        dnsMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte());
        dnsMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte());
        dnsMetricLog.setInternal_query_num(cnRecordLog.getInternal_query_num());
        dnsMetricLog.setExternal_query_num(cnRecordLog.getExternal_query_num());
    }

    private void updateMetric(CnRecordLog cnRecordLog, DnsMetricLog dnsMetricLog) {
        dnsMetricLog.setCommon_sessions(cnRecordLog.getCommon_sessions() + dnsMetricLog.getCommon_sessions());
        dnsMetricLog.setDns_count(cnRecordLog.getDns_count() + dnsMetricLog.getDns_count());
        dnsMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms() + dnsMetricLog.getDns_response_latency_ms());
        dnsMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte() + dnsMetricLog.getTraffic_outbound_byte());
        dnsMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte() + dnsMetricLog.getTraffic_inbound_byte());
        dnsMetricLog.setInternal_query_num(cnRecordLog.getInternal_query_num() + dnsMetricLog.getInternal_query_num());
        dnsMetricLog.setExternal_query_num(cnRecordLog.getExternal_query_num() + dnsMetricLog.getExternal_query_num());
    }
}
