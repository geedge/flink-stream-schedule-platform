package com.zdjizhi.dns.pre.process;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.net.InternetDomainName;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.dns.pre.common.DnsMetricLog;
import com.zdjizhi.utils.FormatUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FirstAggregationProcess extends ProcessWindowFunction<CnRecordLog, DnsMetricLog, String, TimeWindow> {
    private static  Logger LOG = LoggerFactory.getLogger(FirstAggregationProcess.class);
    private MapState<String, DnsMetricLog> dnsServerIpMetricMap;
    private MapState<String, DnsMetricLog> dnsQnameMetricMap;
    private MapState<Long, DnsMetricLog> dnsQtypeMetricMap;
    private MapState<Long, DnsMetricLog> dnsRcodeMtricMap;
    private MapState<String, DnsMetricLog> dnsRraMtricMap;
    private MapState<String, DnsMetricLog> dnsRraaaaMtricMap;
    private MapState<String, DnsMetricLog> dnsRrcnameMtricMap;


    @Override
    public void open(Configuration parameters) throws Exception {
        dnsServerIpMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("dns_server_ip_metric_map",
                        Types.STRING,
                        Types.POJO(DnsMetricLog.class)));

        dnsQnameMetricMap=getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, DnsMetricLog>("dns_qname_metric_map",
                        Types.STRING,
                        Types.POJO(DnsMetricLog.class)));

        dnsQtypeMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("dns_qtype_metric_map",
                        Types.LONG,
                        Types.POJO(DnsMetricLog.class)));

        dnsRcodeMtricMap=getRuntimeContext()
                .getMapState(new MapStateDescriptor<Long, DnsMetricLog>("dns_rcode_metric_map",
                        Types.LONG,
                        Types.POJO(DnsMetricLog.class)));

        dnsRraMtricMap=getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, DnsMetricLog>("dns_rra_metric_map",
                        Types.STRING,
                        Types.POJO(DnsMetricLog.class)));

        dnsRraaaaMtricMap=getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, DnsMetricLog>("dns_rraaaa_metric_map",
                        Types.STRING,
                        Types.POJO(DnsMetricLog.class)));

        dnsRrcnameMtricMap=getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, DnsMetricLog>("dns_rrcname_metric_map",
                        Types.STRING,
                        Types.POJO(DnsMetricLog.class)));

    }

    @Override
    public void process(String s, Context context, Iterable<CnRecordLog> elements, Collector<DnsMetricLog> out) throws Exception {
        for (CnRecordLog element : elements) {
            String commonServerIp = element.getCommon_server_ip();
            DnsMetricLog serverIpMetric = dnsServerIpMetricMap.get(commonServerIp);
            if (serverIpMetric != null){
                updateMetric(element,serverIpMetric);
            }else{
                serverIpMetric=new DnsMetricLog();
                initMetric(element,serverIpMetric);
                serverIpMetric.setCommon_server_ip(element.getCommon_server_ip());
                serverIpMetric.setServer_country(element.getServer_country());
                serverIpMetric.setServer_city(element.getServer_region());
                serverIpMetric.setServer_isp(element.getServer_isp());
                serverIpMetric.setDns_server_org(element.getDns_server_org());
                serverIpMetric.setDns_server_role(element.getDns_server_role());
            }
            dnsServerIpMetricMap.put(commonServerIp,serverIpMetric);

            try {
                if (StringUtils.isNotBlank(element.getDns_qname())){
                   String dnsQname = element.getDns_qname();
                    String secondDomain=null;
                    String topDomain=null;
                    try {
                        //获取二级域名
                        secondDomain = FormatUtils.getTopPrivateDomain(dnsQname);
                    } catch (StringIndexOutOfBoundsException outOfBoundsException){
                        LOG.error("Parse top-level domain exceptions, exception domain names:" + dnsQname);
                    }

//                    String secondDomain = FormatUtils.getTopPrivateDomain(dnsQname);
                    //获取公有顶级域名
                    InternetDomainName internetDomainName = InternetDomainName.from(dnsQname);
                    //判断是否有公有顶级域
                    if (internetDomainName.hasPublicSuffix()){
                        topDomain = internetDomainName.publicSuffix().toString();
                    }
                    DnsMetricLog dnsQnameMetric = dnsQnameMetricMap.get(dnsQname);
                    if (dnsQnameMetric !=null){
                        updateMetric(element,dnsQnameMetric);
                    }else{
                        dnsQnameMetric = new DnsMetricLog();
                        initMetric(element,dnsQnameMetric);
                        dnsQnameMetric.setDns_qname(dnsQname);
                        dnsQnameMetric.setQname_sld(secondDomain);
                        dnsQnameMetric.setQname_tld(topDomain);
                    }
                    dnsQnameMetricMap.put(dnsQname,dnsQnameMetric);
                }
            }
            catch (IllegalArgumentException e){
                LOG.warn("Illegal domain name"+ e);
            }


            long dnsQtype = element.getDns_qtype();
            DnsMetricLog dnsQtypeMetric = dnsQtypeMetricMap.get(dnsQtype);
            if (dnsQtypeMetric != null){
                updateMetric(element,dnsQtypeMetric);
            }else{
                dnsQtypeMetric=new DnsMetricLog();
                initMetric(element,dnsQtypeMetric);
                dnsQtypeMetric.setDns_qtype(dnsQtype);
            }
            dnsQtypeMetricMap.put(dnsQtype,dnsQtypeMetric);

            long dnsRcode = element.getDns_rcode();
            DnsMetricLog dnsRcodeMetric = dnsRcodeMtricMap.get(dnsRcode);
            if (dnsRcodeMetric != null){
                updateMetric(element,dnsRcodeMetric);
            }else {
                dnsRcodeMetric=new DnsMetricLog();
                initMetric(element,dnsRcodeMetric);
                dnsRcodeMetric.setDns_rcode(dnsRcode);
            }
            dnsRcodeMtricMap.put(dnsRcode,dnsRcodeMetric);

            String dns_rr = element.getDns_rr();
            if (StringUtil.isNotBlank(dns_rr)){
                HashMap<String, Object> o = JSON.parseObject(dns_rr, TypeReference.mapType(HashMap.class, String.class, Object.class));
                List<HashMap<String, Object>> rr = JSON.parseArray(JSON.toJSONString(o.get("rr")), TypeReference.mapType(HashMap.class, String.class, Object.class));
                for (HashMap<String, Object> obj : rr) {
                    if (1==Integer.parseInt(obj.get("type").toString())){
                        if (obj.containsKey("a")){
                            if (StringUtil.isNotBlank(obj.get("a").toString())){
                                String rrA = obj.get("a").toString();
                                DnsMetricLog dnsRraMetric = dnsRraMtricMap.get(rrA);
                                if (dnsRraMetric != null){
                                    updateMetric(element,dnsRraMetric);
                                }else {
                                    dnsRraMetric=new DnsMetricLog();
                                    initMetric(element,dnsRraMetric);
                                    dnsRraMetric.setDns_rra(rrA);
                                }
                                dnsRraMtricMap.put(rrA,dnsRraMetric);
                            }
                        }
                    }
                    else if (28==Integer.parseInt(obj.get("type").toString())){
                        if (obj.containsKey("aaaa")){
                            if (StringUtil.isNotBlank(obj.get("aaaa").toString())){
                                String rrAaaa = obj.get("aaaa").toString();
                                DnsMetricLog dnsRraaaaMetric = dnsRraaaaMtricMap.get(rrAaaa);
                                if (dnsRraaaaMetric != null){
                                    updateMetric(element,dnsRraaaaMetric);
                                }else {
                                    dnsRraaaaMetric=new DnsMetricLog();
                                    initMetric(element,dnsRraaaaMetric);
                                    dnsRraaaaMetric.setDns_rraaaa(rrAaaa);
                                }
                                dnsRraaaaMtricMap.put(rrAaaa,dnsRraaaaMetric);
                            }
                        }
                    }
                    else if (5==Integer.parseInt(obj.get("type").toString())){
                        if (obj.containsKey("cname")){
                            if (StringUtil.isNotBlank(obj.get("cname").toString())){
                                String rrCname = obj.get("cname").toString();
                                DnsMetricLog dnsRrCnameMetric = dnsRrcnameMtricMap.get(rrCname);
                                if (dnsRrCnameMetric != null){
                                    updateMetric(element,dnsRrCnameMetric);
                                }else {
                                    dnsRrCnameMetric=new DnsMetricLog();
                                    initMetric(element,dnsRrCnameMetric);
                                    dnsRrCnameMetric.setDns_rrcname(rrCname);
                                }
                                dnsRrcnameMtricMap.put(rrCname,dnsRrCnameMetric);
                            }
                        }
                    }
                }
            }
        }



        for (DnsMetricLog value : dnsServerIpMetricMap.values()) {
            out.collect(value);
        }

        for (DnsMetricLog value : dnsQnameMetricMap.values()) {
            out.collect(value);
        }

        for (DnsMetricLog value : dnsQtypeMetricMap.values()) {
            out.collect(value);
        }

        for (DnsMetricLog value : dnsRcodeMtricMap.values()) {
            out.collect(value);
        }

        for (DnsMetricLog value : dnsRraMtricMap.values()) {
            out.collect(value);
        }

        for (DnsMetricLog value : dnsRraaaaMtricMap.values()) {
            out.collect(value);
        }

        for (DnsMetricLog value : dnsRrcnameMtricMap.values()) {
            out.collect(value);
        }

        dnsServerIpMetricMap.clear();
        dnsQnameMetricMap.clear();
        dnsQtypeMetricMap.clear();
        dnsRcodeMtricMap.clear();
        dnsRraMtricMap.clear();
        dnsRraaaaMtricMap.clear();
        dnsRrcnameMtricMap.clear();
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
        dnsMetricLog.setDns_count(cnRecordLog.getDns_count()+dnsMetricLog.getDns_count());
        dnsMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms()+dnsMetricLog.getDns_response_latency_ms());
        dnsMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte() + dnsMetricLog.getTraffic_outbound_byte());
        dnsMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte() + dnsMetricLog.getTraffic_inbound_byte());
        dnsMetricLog.setInternal_query_num(cnRecordLog.getInternal_query_num()+dnsMetricLog.getInternal_query_num());
        dnsMetricLog.setExternal_query_num(cnRecordLog.getExternal_query_num()+dnsMetricLog.getExternal_query_num());
    }

}

