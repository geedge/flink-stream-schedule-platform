package com.zdjizhi.pre.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirstAggregationFunc implements AggregateFunction<CnRecordLog, Map<String, Map<String, CnMetricLog>>, List<CnMetricLog>> {

    private static final String clientIpMetricKey = "clientIpMetric";
    private final String serverIpMetricKey = "serverIpMetric";
    private final String clientRegionMetricKey = "clientRegionMetric";
    private final String serverRegionMetricKey = "serverRegionMetric";
    private final String clientAsnMetricKey = "clientAsnMetric";
    private final String serverAsnMetricKey = "serverAsnMetric";
    private final String clientIdcMetricKey = "clientIdcMetric";
    private final String serverIdcMetricKey = "serverIdcMetric";
    private final String appMetricKey = "appMetric";
    private final String domainMetricKey = "domainMetric";
    private final String httpHostMetricKey = "httpHostMetric";
    private final String sslSniMetricKey = "sslSniMetric";
    private final String protocolMetricKey = "protocolMetric";
    private final String linkMetricKey = "linkMetric";

    @Override
    public Map<String, Map<String, CnMetricLog>> createAccumulator() {
        Map<String, Map<String, CnMetricLog>> accumulator = new HashMap<>();
        accumulator.put(clientIpMetricKey, new HashMap<>());
        accumulator.put(serverIpMetricKey, new HashMap<>());
        accumulator.put(clientRegionMetricKey, new HashMap<>());
        accumulator.put(serverRegionMetricKey, new HashMap<>());
        accumulator.put(clientAsnMetricKey, new HashMap<>());
        accumulator.put(serverAsnMetricKey, new HashMap<>());
        accumulator.put(clientIdcMetricKey, new HashMap<>());
        accumulator.put(serverIdcMetricKey, new HashMap<>());
        accumulator.put(appMetricKey, new HashMap<>());
        accumulator.put(domainMetricKey, new HashMap<>());
        accumulator.put(httpHostMetricKey, new HashMap<>());
        accumulator.put(sslSniMetricKey, new HashMap<>());
        accumulator.put(protocolMetricKey, new HashMap<>());
        accumulator.put(linkMetricKey, new HashMap<>());
        return accumulator;
    }

    @Override
    public Map<String, Map<String, CnMetricLog>> add(CnRecordLog value, Map<String, Map<String, CnMetricLog>> accumulator) {
        try {
            String commonClientIp = value.getCommon_client_ip();
            Map<String, CnMetricLog> clientIpMetricMap = accumulator.get(clientIpMetricKey);
            CnMetricLog clientIpMetric = clientIpMetricMap.get(commonClientIp);
            if (clientIpMetric != null) {
                updateMetric(value, clientIpMetric);
            } else {
                clientIpMetric = new CnMetricLog();

                initMetric(value, clientIpMetric);
                clientIpMetric.setCommon_client_ip(commonClientIp);
                clientIpMetric.setZone(value.getClient_zone());
            }
            clientIpMetricMap.put(commonClientIp, clientIpMetric);

            String commonServerIp = value.getCommon_server_ip();
            Map<String, CnMetricLog> serverIpMetricMap = accumulator.get(serverIpMetricKey);
            CnMetricLog serverIpMetric = serverIpMetricMap.get(commonServerIp);
            if (serverIpMetric != null) {
                updateMetric(value, serverIpMetric);
            } else {
                serverIpMetric = new CnMetricLog();
                initMetric(value, serverIpMetric);
                serverIpMetric.setCommon_server_ip(commonServerIp);
                serverIpMetric.setZone(value.getServer_zone());
            }
            serverIpMetricMap.put(commonServerIp, serverIpMetric);

            String clientCountry = value.getClient_country();
            String clientProvince = value.getClient_province();
            String clientRegion = value.getClient_region();
            if (StringUtil.isNotBlank(clientCountry) || StringUtil.isNotBlank(clientProvince) || StringUtil.isNotBlank(clientRegion)){
                String regionKey = clientCountry + "-" + clientProvince + "-" + clientRegion;
                Map<String, CnMetricLog> clientRegionMetricMap = accumulator.get(clientRegionMetricKey);
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
                Map<String, CnMetricLog> serverRegionMetricMap = accumulator.get(serverRegionMetricKey);
                CnMetricLog metricLog = serverRegionMetricMap.get(regionKey);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setServer_country(serverCountry);
                    metricLog.setServer_province(serverProvince);
                    metricLog.setServer_region(serverRegion);
                }
                serverRegionMetricMap.put(regionKey,metricLog);
            }

            String clientAsn = value.getClient_asn();
            String clientIsp = value.getClient_isp();
            if (StringUtil.isNotBlank(clientAsn) || StringUtil.isNotBlank(clientIsp)){
                String key = clientAsn + "-" + clientIsp;
                Map<String, CnMetricLog> clientAsnMetricMap = accumulator.get(clientAsnMetricKey);
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
                Map<String, CnMetricLog> serverAsnMetricMap = accumulator.get(serverAsnMetricKey);
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
                Map<String, CnMetricLog> clientIdcMetricMap = accumulator.get(clientIdcMetricKey);
                CnMetricLog metricLog = clientIdcMetricMap.get(clientIdcRenter);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setClient_idc_renter(clientIdcRenter);
                }
                clientIdcMetricMap.put(clientIdcRenter,metricLog);
            }

            String serverIdcRenter = value.getServer_idc_renter();
            if (StringUtil.isNotBlank(serverIdcRenter)){
                Map<String, CnMetricLog> serverIdcMetricMap = accumulator.get(serverIdcMetricKey);
                CnMetricLog metricLog = serverIdcMetricMap.get(serverIdcRenter);
                if (metricLog != null){
                    updateMetric(value,metricLog);
                }else {
                    metricLog = new CnMetricLog();
                    initMetric(value,metricLog);
                    metricLog.setServer_idc_renter(serverIdcRenter);
                }
                serverIdcMetricMap.put(serverIdcRenter,metricLog);
            }

            String commonAppLabel = value.getCommon_app_label();
            if (StringUtil.isNotBlank(commonAppLabel)){
                Map<String, CnMetricLog> appMetricMap = accumulator.get(appMetricKey);
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
                Map<String, CnMetricLog> domainMetricMap = accumulator.get(domainMetricKey);
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
                Map<String, CnMetricLog> httpHostMetricMap = accumulator.get(httpHostMetricKey);
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
                Map<String, CnMetricLog> sslSniMetricMap = accumulator.get(sslSniMetricKey);
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
                Map<String, CnMetricLog> protocolMetricMap = accumulator.get(protocolMetricKey);
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
                Map<String, CnMetricLog> linkMetricMap = accumulator.get(linkMetricKey);
                CnMetricLog metricLog = linkMetricMap.get(key);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog();
                    initMetric(value, metricLog);
                    metricLog.setClient_country(client_Country);
                    metricLog.setClient_province(client_Province);
                    metricLog.setClient_region(client_Region);
                    metricLog.setClient_zone(value.getClient_zone());
                    metricLog.setServer_country(server_Country);
                    metricLog.setServer_province(server_Province);
                    metricLog.setServer_region(server_Region);
                    metricLog.setServer_zone(value.getServer_zone());
                    metricLog.setCommon_egress_link_id(value.getCommon_egress_link_id());
                    metricLog.setCommon_ingress_link_id(value.getCommon_ingress_link_id());
                    metricLog.setIngress_link_direction(value.getIngress_link_direction());
                    metricLog.setEgress_link_direction(value.getEgress_link_direction());
                }
                linkMetricMap.put(key, metricLog);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return accumulator;
    }

    @Override
    public List<CnMetricLog> getResult(Map<String, Map<String, CnMetricLog>> accumulator) {
        List<CnMetricLog> result = new ArrayList<>();
        for (Map<String, CnMetricLog> value : accumulator.values()) {
            result.addAll(value.values());
        }
        return result;
    }

    @Override
    public Map<String, Map<String, CnMetricLog>> merge(Map<String, Map<String, CnMetricLog>> a, Map<String, Map<String, CnMetricLog>> b) {
        return null;
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
        cnMetricLog.setCommon_s2c_pkt_retrans(cnMetricLog.getCommon_s2c_pkt_retrans());
        cnMetricLog.setCommon_establish_latency_ms(cnRecordLog.getCommon_establish_latency_ms());
        cnMetricLog.setHttp_response_latency_ms(cnRecordLog.getHttp_response_latency_ms());
        cnMetricLog.setSsl_con_latency_ms(cnRecordLog.getSsl_con_latency_ms());
//        cnMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms());
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
        cnMetricLog.setCommon_s2c_pkt_retrans(cnRecordLog.getCommon_s2c_pkt_retrans()+cnMetricLog.getCommon_s2c_pkt_retrans());
        cnMetricLog.setCommon_establish_latency_ms(cnRecordLog.getCommon_establish_latency_ms() + cnMetricLog.getCommon_establish_latency_ms());
        cnMetricLog.setHttp_response_latency_ms(cnRecordLog.getHttp_response_latency_ms() + cnMetricLog.getHttp_response_latency_ms());
        cnMetricLog.setSsl_con_latency_ms(cnRecordLog.getSsl_con_latency_ms() + cnMetricLog.getSsl_con_latency_ms());
//        cnMetricLog.setDns_response_latency_ms(cnRecordLog.getDns_response_latency_ms() + cnMetricLog.getDns_response_latency_ms());
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
