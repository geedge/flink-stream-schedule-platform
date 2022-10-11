package com.zdjizhi.pre.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FirstAggregationProcess extends ProcessWindowFunction<CnRecordLog, CnMetricLog, String, TimeWindow> {

    private MapState<String, CnMetricLog> clientIpMetricMap;
    private MapState<String, CnMetricLog> serverIpMetricMap;
    private MapState<String, CnMetricLog> clientRegionMetricMap;
    private MapState<String, CnMetricLog> serverRegionMetricMap;
    private MapState<String, CnMetricLog> clientAsnMetricMap;
    private MapState<String, CnMetricLog> serverAsnMetricMap;
    private MapState<String, CnMetricLog> clientIdcMetricMap;
    private MapState<String, CnMetricLog> serverIdcMetricMap;
    private MapState<String, CnMetricLog> appMetricMap;
    private MapState<String, CnMetricLog> domainMetricMap;
    private MapState<String, CnMetricLog> httpHostMetricMap;
    private MapState<String, CnMetricLog> sslSniMetricMap;
    private MapState<String, CnMetricLog> protocolMetricMap;
    private MapState<String, CnMetricLog> linkMetricMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        clientIpMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("client_ip_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        serverIpMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("server_ip_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        clientRegionMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("client_region_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        serverRegionMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("server_region_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        clientAsnMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("client_asn_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        serverAsnMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("server_asn_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        clientIdcMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("client_idc_renter_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        serverIdcMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("server_idc_renter_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        appMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("application_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        domainMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("domain_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        httpHostMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("http_host_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        sslSniMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("ssl_sni_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        protocolMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("protocol_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));

        linkMetricMap = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("link_metric_map",
                        Types.STRING,
                        Types.POJO(CnMetricLog.class)));
    }

    @Override
    public void process(String s, Context context, Iterable<CnRecordLog> elements, Collector<CnMetricLog> out) throws Exception {
        for (CnRecordLog value:elements){

            String commonClientIp = value.getCommon_client_ip();
            CnMetricLog clientIpMetric = clientIpMetricMap.get(commonClientIp);
            if (clientIpMetric != null) {
                updateMetric(value, clientIpMetric);
            } else {
                clientIpMetric = new CnMetricLog();
                initMetric(value, clientIpMetric);
                clientIpMetric.setCommon_client_ip(commonClientIp);
                clientIpMetric.setClient_zone(value.getClient_zone());
            }
            clientIpMetricMap.put(commonClientIp, clientIpMetric);

            String commonServerIp = value.getCommon_server_ip();
            CnMetricLog serverIpMetric = serverIpMetricMap.get(commonServerIp);
            if (serverIpMetric != null) {
                updateMetric(value, serverIpMetric);
            } else {
                serverIpMetric = new CnMetricLog();
                initMetric(value, serverIpMetric);
                serverIpMetric.setCommon_server_ip(commonServerIp);
                serverIpMetric.setServer_zone(value.getServer_zone());
            }
            serverIpMetricMap.put(commonServerIp, serverIpMetric);

            String clientCountry = value.getClient_country();
            String clientProvince = value.getClient_province();
            String clientRegion = value.getClient_region();
            if (StringUtil.isNotBlank(clientCountry) || StringUtil.isNotBlank(clientProvince) || StringUtil.isNotBlank(clientRegion)){
                String regionKey = clientCountry + "-" + clientProvince + "-" + clientRegion;
                CnMetricLog metricLog = clientRegionMetricMap.get(regionKey);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setClient_country(clientCountry);
                    metricLog.setClient_province(clientProvince);
                    metricLog.setClient_region(clientRegion);
                }
                clientRegionMetricMap.put(regionKey,metricLog);
            }

            String serverCountry = value.getServer_country();
            String serverProvince = value.getServer_province();
            String serverRegion = value.getServer_region();
            if (StringUtil.isNotBlank(serverCountry) || StringUtil.isNotBlank(serverProvince) || StringUtil.isNotBlank(serverRegion)){
                String regionKey = serverCountry + "-" + serverProvince + "-" + serverRegion;
                CnMetricLog metricLog = serverIpMetricMap.get(regionKey);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setServer_country(serverCountry);
                    metricLog.setServer_province(serverProvince);
                    metricLog.setServer_region(serverRegion);
                }
                serverIpMetricMap.put(regionKey,metricLog);
            }

            String clientAsn = value.getClient_asn();
            String clientIsp = value.getClient_isp();
            if (StringUtil.isNotBlank(clientAsn) || StringUtil.isNotBlank(clientIsp)){
                String key = clientAsn + "-" + clientIsp;
                CnMetricLog metricLog = clientAsnMetricMap.get(key);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setClient_asn(clientAsn);
                    metricLog.setClient_isp(clientIsp);
                }
                clientAsnMetricMap.put(key,metricLog);
            }

            String serverAsn = value.getServer_asn();
            String serverIsp = value.getServer_isp();
            if (StringUtil.isNotBlank(serverAsn) || StringUtil.isNotBlank(serverIsp)){
                String key = serverAsn + "-" + serverIsp;
                CnMetricLog metricLog = serverAsnMetricMap.get(key);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setServer_asn(serverAsn);
                    metricLog.setServer_isp(serverIsp);
                }
                serverAsnMetricMap.put(key,metricLog);
            }

            String clientIdcRenter = value.getClient_idc_renter();
            if (StringUtil.isNotBlank(clientIdcRenter)){
                CnMetricLog metricLog = serverAsnMetricMap.get(clientIdcRenter);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setClient_idc_renter(clientIdcRenter);
                }
                serverAsnMetricMap.put(clientIdcRenter,metricLog);
            }

            String serverIdcRenter = value.getServer_idc_renter();
            if (StringUtil.isNotBlank(serverIdcRenter)){
                CnMetricLog metricLog = serverAsnMetricMap.get(serverIdcRenter);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setServer_idc_renter(serverIdcRenter);
                }
                serverAsnMetricMap.put(serverIdcRenter,metricLog);
            }

            String commonAppLabel = value.getCommon_app_label();
            if (StringUtil.isNotBlank(commonAppLabel)){
                CnMetricLog metricLog = appMetricMap.get(commonAppLabel);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setCommon_app_label(commonAppLabel);
                    metricLog.setApp_category(value.getApp_category());
                    metricLog.setApp_company(value.getApp_company());
                    metricLog.setApp_subcategory(value.getApp_subcategory());
                }
                appMetricMap.put(commonAppLabel,metricLog);
            }

            String domain = value.getDomain();
            if (StringUtil.isNotBlank(domain)){
                CnMetricLog metricLog = domainMetricMap.get(domain);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setDomain(domain);
                    metricLog.setDomain_category_group(value.getDomain_category_group());
                    metricLog.setDomain_category_name(value.getDomain_category_name());
                }
                domainMetricMap.put(domain,metricLog);
            }

            String httpHost = value.getHttp_host();
            if (StringUtil.isNotBlank(httpHost)){
                CnMetricLog metricLog = httpHostMetricMap.get(httpHost);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setHttp_host(httpHost);
                }
                httpHostMetricMap.put(httpHost,metricLog);
            }

            String sslSni = value.getSsl_sni();
            if (StringUtil.isNotBlank(sslSni)){
                CnMetricLog metricLog = sslSniMetricMap.get(sslSni);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setSsl_sni(sslSni);
                }
                sslSniMetricMap.put(sslSni,metricLog);
            }

            String commonL7Protocol = value.getCommon_l7_protocol();
            int commonServerPort = value.getCommon_server_port();
            if (StringUtil.isNotBlank(commonL7Protocol)){
                String key = commonL7Protocol + "-" + commonServerPort;
                CnMetricLog metricLog = protocolMetricMap.get(key);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setCommon_l7_protocol(commonL7Protocol);
                    metricLog.setCommon_server_port(commonServerPort);
                }
                protocolMetricMap.put(key,metricLog);
            }

            String client_Country = value.getClient_country();
            String client_Province = value.getClient_province();
            String client_Region = value.getClient_region();
            String server_Country = value.getServer_country();
            String server_Province = value.getServer_province();
            String server_Region = value.getServer_region();
            if (StringUtil.isNotBlank(client_Country) || StringUtil.isNotBlank(client_Province) || StringUtil.isNotBlank(client_Region) || StringUtil.isNotBlank(server_Country) || StringUtil.isNotBlank(server_Province) || StringUtil.isNotBlank(server_Region)) {
                String key = client_Country + "-" + client_Province + "-" +client_Region + "-" +server_Country + "-" + server_Province +"-" +server_Region;
                CnMetricLog metricLog = linkMetricMap.get(key);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog();
                    initMetric(value, metricLog);
                    metricLog.setClient_country(client_Country);
                    metricLog.setClient_province(client_Province);
                    metricLog.setClient_region(client_Region);
                    metricLog.setServer_country(server_Country);
                    metricLog.setServer_province(server_Province);
                    metricLog.setServer_region(server_Region);
                    metricLog.setCommon_egress_link_id(value.getCommon_egress_link_id());
                    metricLog.setCommon_ingress_link_id(value.getCommon_ingress_link_id());
                    metricLog.setIngress_link_direction(value.getIngress_link_direction());
                    metricLog.setEgress_link_direction(value.getEgress_link_direction());
                }
                linkMetricMap.put(key, metricLog);
            }
        }

        for (CnMetricLog next : clientIpMetricMap.values()) {
            out.collect(next);
        }

        for (CnMetricLog next : serverIpMetricMap.values()) {
            out.collect(next);
        }

        for (CnMetricLog next : clientRegionMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : serverRegionMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : clientAsnMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : serverAsnMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : clientIdcMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : serverIdcMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : appMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : domainMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : httpHostMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : sslSniMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog next : protocolMetricMap.values()){
            out.collect(next);
        }

        for (CnMetricLog value : linkMetricMap.values()) {
            out.collect(value);
        }
        clientIpMetricMap.clear();
        serverIpMetricMap.clear();
        clientRegionMetricMap.clear();
        serverRegionMetricMap.clear();
        clientAsnMetricMap.clear();
        serverAsnMetricMap.clear();
        clientIdcMetricMap.clear();
        serverIdcMetricMap.clear();
        appMetricMap.clear();
        domainMetricMap.clear();
        httpHostMetricMap.clear();
        sslSniMetricMap.clear();
        protocolMetricMap.clear();
        linkMetricMap.clear();
    }

    private void initMetric(CnRecordLog cnRecordLog, CnMetricLog cnMetricLog) {
        cnMetricLog.setCommon_start_time(cnRecordLog.getCommon_start_time());
        cnMetricLog.setCommon_recv_time(cnRecordLog.getCommon_recv_time());
        cnMetricLog.setCommon_c2s_pkt_num(cnRecordLog.getCommon_c2s_pkt_num());
        cnMetricLog.setCommon_s2c_pkt_num(cnRecordLog.getCommon_s2c_pkt_num());
        cnMetricLog.setCommon_c2s_byte_num(cnRecordLog.getCommon_c2s_byte_num());
        cnMetricLog.setCommon_s2c_byte_num(cnRecordLog.getCommon_s2c_byte_num());
        cnMetricLog.setCommon_sessions(cnRecordLog.getCommon_sessions());
        cnMetricLog.setCommon_c2s_tcp_lostlen(cnRecordLog.getCommon_c2s_tcp_lostlen());
        cnMetricLog.setCommon_s2c_tcp_lostlen(cnRecordLog.getCommon_s2c_tcp_lostlen());
        cnMetricLog.setCommon_c2s_tcp_unorder_num(cnRecordLog.getCommon_c2s_tcp_unorder_num());
        cnMetricLog.setCommon_s2c_tcp_unorder_num(cnRecordLog.getCommon_s2c_tcp_unorder_num());
        cnMetricLog.setCommon_c2s_byte_retrans(cnRecordLog.getCommon_c2s_byte_retrans());
        cnMetricLog.setCommon_s2c_byte_retrans(cnRecordLog.getCommon_s2c_byte_retrans());
        cnMetricLog.setCommon_c2s_pkt_retrans(cnRecordLog.getCommon_c2s_pkt_retrans());
        cnMetricLog.setCommon_establish_latency_ms(cnRecordLog.getCommon_establish_latency_ms());
        cnMetricLog.setHttp_response_latency_ms(cnRecordLog.getHttp_response_latency_ms());
        cnMetricLog.setSsl_con_latency_ms(cnRecordLog.getSsl_con_latency_ms());
        cnMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms());
        cnMetricLog.setC2s_tcp_pkt_num(cnRecordLog.getC2s_tcp_pkt_num());
        cnMetricLog.setS2c_tcp_pkt_num(cnRecordLog.getS2c_tcp_pkt_num());
        cnMetricLog.setC2s_tcp_byte_num(cnRecordLog.getC2s_tcp_byte_num());
        cnMetricLog.setS2c_tcp_byte_num(cnRecordLog.getS2c_tcp_byte_num());
        cnMetricLog.setTcp_count(cnRecordLog.getTcp_count());
        cnMetricLog.setHttp_count(cnRecordLog.getHttp_count());
        cnMetricLog.setSsl_count(cnRecordLog.getSsl_count());
        cnMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte());
        cnMetricLog.setTraffic_inbound_pkt(cnRecordLog.getTraffic_inbound_pkt());
        cnMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte());
        cnMetricLog.setTraffic_outbound_pkt(cnRecordLog.getTraffic_outbound_pkt());
        cnMetricLog.setTraffic_internal_byte(cnRecordLog.getTraffic_internal_byte());
        cnMetricLog.setTraffic_internal_pkt(cnRecordLog.getTraffic_internal_pkt());
        cnMetricLog.setTraffic_through_byte(cnRecordLog.getTraffic_through_byte());
        cnMetricLog.setTraffic_through_pkt(cnRecordLog.getTraffic_through_pkt());
    }

    private void updateMetric(CnRecordLog cnRecordLog, CnMetricLog cnMetricLog) {
        cnMetricLog.setCommon_c2s_pkt_num(cnRecordLog.getCommon_c2s_pkt_num() + cnMetricLog.getCommon_c2s_pkt_num());
        cnMetricLog.setCommon_s2c_pkt_num(cnRecordLog.getCommon_s2c_pkt_num() + cnMetricLog.getCommon_s2c_pkt_num());
        cnMetricLog.setCommon_c2s_byte_num(cnRecordLog.getCommon_c2s_byte_num() + cnMetricLog.getCommon_c2s_byte_num());
        cnMetricLog.setCommon_s2c_byte_num(cnRecordLog.getCommon_s2c_byte_num() + cnMetricLog.getCommon_s2c_byte_num());
        cnMetricLog.setCommon_sessions(cnRecordLog.getCommon_sessions() + cnMetricLog.getCommon_sessions());
        cnMetricLog.setCommon_c2s_tcp_lostlen(cnRecordLog.getCommon_c2s_tcp_lostlen() + cnMetricLog.getCommon_c2s_tcp_lostlen());
        cnMetricLog.setCommon_s2c_tcp_lostlen(cnRecordLog.getCommon_s2c_tcp_lostlen() + cnMetricLog.getCommon_s2c_tcp_lostlen());
        cnMetricLog.setCommon_c2s_tcp_unorder_num(cnRecordLog.getCommon_c2s_tcp_unorder_num() + cnMetricLog.getCommon_c2s_tcp_unorder_num());
        cnMetricLog.setCommon_s2c_tcp_unorder_num(cnRecordLog.getCommon_s2c_tcp_unorder_num() + cnMetricLog.getCommon_s2c_tcp_unorder_num());
        cnMetricLog.setCommon_c2s_byte_retrans(cnRecordLog.getCommon_c2s_byte_retrans() + cnMetricLog.getCommon_c2s_byte_retrans());
        cnMetricLog.setCommon_s2c_byte_retrans(cnRecordLog.getCommon_s2c_byte_retrans() + cnMetricLog.getCommon_s2c_byte_retrans());
        cnMetricLog.setCommon_c2s_pkt_retrans(cnRecordLog.getCommon_c2s_pkt_retrans() + cnMetricLog.getCommon_c2s_pkt_retrans());
        cnMetricLog.setCommon_establish_latency_ms(cnRecordLog.getCommon_establish_latency_ms() + cnMetricLog.getCommon_establish_latency_ms());
        cnMetricLog.setHttp_response_latency_ms(cnRecordLog.getHttp_response_latency_ms() + cnMetricLog.getHttp_response_latency_ms());
        cnMetricLog.setSsl_con_latency_ms(cnRecordLog.getSsl_con_latency_ms() + cnMetricLog.getSsl_con_latency_ms());
        cnMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms() + cnMetricLog.getDns_response_latency_ms());
        cnMetricLog.setC2s_tcp_pkt_num(cnRecordLog.getC2s_tcp_pkt_num() + cnMetricLog.getC2s_tcp_pkt_num());
        cnMetricLog.setS2c_tcp_pkt_num(cnRecordLog.getS2c_tcp_pkt_num() + cnMetricLog.getS2c_tcp_pkt_num());
        cnMetricLog.setC2s_tcp_byte_num(cnRecordLog.getC2s_tcp_byte_num() + cnMetricLog.getC2s_tcp_byte_num());
        cnMetricLog.setS2c_tcp_byte_num(cnRecordLog.getS2c_tcp_byte_num() + cnMetricLog.getS2c_tcp_byte_num());
        cnMetricLog.setTcp_count(cnRecordLog.getTcp_count() + cnMetricLog.getTcp_count());
        cnMetricLog.setHttp_count(cnRecordLog.getHttp_count() + cnMetricLog.getHttp_count());
        cnMetricLog.setSsl_count(cnRecordLog.getSsl_count() + cnMetricLog.getSsl_count());
        cnMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte() + cnMetricLog.getTraffic_inbound_byte());
        cnMetricLog.setTraffic_inbound_pkt(cnRecordLog.getTraffic_inbound_pkt() + cnMetricLog.getTraffic_inbound_pkt());
        cnMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte() + cnMetricLog.getTraffic_outbound_byte());
        cnMetricLog.setTraffic_outbound_pkt(cnRecordLog.getTraffic_outbound_pkt() + cnMetricLog.getTraffic_outbound_pkt());
        cnMetricLog.setTraffic_internal_byte(cnRecordLog.getTraffic_internal_byte() + cnMetricLog.getTraffic_internal_byte());
        cnMetricLog.setTraffic_internal_pkt(cnRecordLog.getTraffic_internal_pkt() + cnMetricLog.getTraffic_internal_pkt());
        cnMetricLog.setTraffic_through_byte(cnRecordLog.getTraffic_through_byte() + cnMetricLog.getTraffic_through_byte());
        cnMetricLog.setTraffic_through_pkt(cnRecordLog.getTraffic_through_pkt() + cnMetricLog.getTraffic_through_pkt());
    }


}
