package com.zdjizhi.pre.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricKeyConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class FirstAggregation extends AbstractFirstAggregation<CnMetricLog> {

    private final String clientIpMetricKey = "clientIpMetric";
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
    private final String subscriberKey = "subscriberMetric";
    private final String subscriberAppKey = "subscriberAppMetric";

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
        accumulator.put(subscriberKey, new HashMap<>());
        accumulator.put(subscriberAppKey, new HashMap<>());

        return accumulator;
    }

    @Override
    public void add(CnRecordLog value, Map<String, Map<String, CnMetricLog>> accumulator) {
        try {
            String commonClientIp = value.getCommon_client_ip();
            Map<String, CnMetricLog> clientIpMetricMap = accumulator.get(clientIpMetricKey);
            CnMetricLog clientIpMetric = clientIpMetricMap.get(commonClientIp);
            if (clientIpMetric != null) {
                updateMetric(value, clientIpMetric);
                updateFlagMetric(value, clientIpMetric);
            } else {
                clientIpMetric = new CnMetricLog(MetricKeyConfig.CLIENT_IP);

                initMetric(value, clientIpMetric);
                initFlagMetric(value, clientIpMetric);
                clientIpMetric.setCommon_client_ip(commonClientIp);
                clientIpMetric.setZone(value.getClient_zone());
            }
            clientIpMetricMap.put(commonClientIp, clientIpMetric);

            String commonServerIp = value.getCommon_server_ip();
            Map<String, CnMetricLog> serverIpMetricMap = accumulator.get(serverIpMetricKey);
            CnMetricLog serverIpMetric = serverIpMetricMap.get(commonServerIp);
            if (serverIpMetric != null) {
                updateMetric(value, serverIpMetric);
                updateFlagMetric(value, serverIpMetric);
            } else {
                serverIpMetric = new CnMetricLog(MetricKeyConfig.SERVER_IP);
                initMetric(value, serverIpMetric);
                initFlagMetric(value, serverIpMetric);
                serverIpMetric.setCommon_server_ip(commonServerIp);
                serverIpMetric.setZone(value.getServer_zone());
            }
            serverIpMetricMap.put(commonServerIp, serverIpMetric);

            String clientCountry = value.getClient_country_region();
            String clientProvince = value.getClient_super_admin_area();
            String clientRegion = value.getClient_admin_area();
            if (StringUtils.isNotBlank(clientCountry) || StringUtils.isNotBlank(clientProvince) || StringUtils.isNotBlank(clientRegion)){
                String regionKey = clientCountry + "-" + clientProvince + "-" + clientRegion;
                Map<String, CnMetricLog> clientRegionMetricMap = accumulator.get(clientRegionMetricKey);
                CnMetricLog metricLog = clientRegionMetricMap.get(regionKey);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.CLIENT_LOCATION);
                    initMetric(value, metricLog);
                    metricLog.setClient_country_region(clientCountry);
                    metricLog.setClient_super_admin_area(clientProvince);
                    metricLog.setClient_admin_area(clientRegion);
                }
                clientRegionMetricMap.put(regionKey, metricLog);
            }

            String serverCountry = value.getServer_country_region();
            String serverProvince = value.getServer_super_admin_area();
            String serverRegion = value.getServer_admin_area();
            if (StringUtils.isNotBlank(serverCountry) || StringUtils.isNotBlank(serverProvince) || StringUtils.isNotBlank(serverRegion)){
                String regionKey = serverCountry + "-" + serverProvince + "-" + serverRegion;
                Map<String, CnMetricLog> serverRegionMetricMap = accumulator.get(serverRegionMetricKey);
                CnMetricLog metricLog = serverRegionMetricMap.get(regionKey);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.SERVER_LOCATION);
                    initMetric(value, metricLog);
                    metricLog.setServer_country_region(serverCountry);
                    metricLog.setServer_super_admin_area(serverProvince);
                    metricLog.setServer_admin_area(serverRegion);
                }
                serverRegionMetricMap.put(regionKey, metricLog);
            }

            String clientAsn = value.getClient_asn();
            String clientIsp = value.getClient_isp();
            if (StringUtils.isNotBlank(clientAsn) || StringUtils.isNotBlank(clientIsp)){
                String key = clientAsn + "-" + clientIsp;
                Map<String, CnMetricLog> clientAsnMetricMap = accumulator.get(clientAsnMetricKey);
                CnMetricLog metricLog = clientAsnMetricMap.get(key);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.CLIENT_ASN_ISP);
                    initMetric(value, metricLog);
                    metricLog.setClient_asn(clientAsn);
                    metricLog.setClient_isp(clientIsp);
                }
                clientAsnMetricMap.put(key, metricLog);
            }

            String serverAsn = value.getServer_asn();
            String serverIsp = value.getServer_isp();
            if (StringUtils.isNotBlank(serverAsn) || StringUtils.isNotBlank(serverIsp)){
                String key = serverAsn + "-" + serverIsp;
                Map<String, CnMetricLog> serverAsnMetricMap = accumulator.get(serverAsnMetricKey);
                CnMetricLog metricLog = serverAsnMetricMap.get(key);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.SERVER_ASN_ISP);
                    initMetric(value, metricLog);
                    metricLog.setServer_asn(serverAsn);
                    metricLog.setServer_isp(serverIsp);
                }
                serverAsnMetricMap.put(key, metricLog);
            }

            String commonAppLabel = value.getCommon_app_label();
            if (StringUtils.isNotBlank(commonAppLabel)) {
                Map<String, CnMetricLog> appMetricMap = accumulator.get(appMetricKey);
                CnMetricLog metricLog = appMetricMap.get(commonAppLabel);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.APP);
                    initMetric(value, metricLog);
                    metricLog.setCommon_app_label(commonAppLabel);
                metricLog.setApp_category(value.getApp_category());
                metricLog.setApp_company(value.getApp_company());
                metricLog.setApp_subcategory(value.getApp_subcategory());
                }
                appMetricMap.put(commonAppLabel, metricLog);
            }


            String domain = value.getDomain();
            if (StringUtils.isNotBlank(domain)) {
                Map<String, CnMetricLog> domainMetricMap = accumulator.get(domainMetricKey);
                CnMetricLog metricLog = domainMetricMap.get(domain);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.DOMAIN);
                    initMetric(value, metricLog);
                    metricLog.setDomain(domain);
                    metricLog.setDomain_sld(value.getDomain_sld());
                    metricLog.setDomain_category_group(value.getDomain_category_group());
                    metricLog.setDomain_category_name(value.getDomain_category_name());
                }
                domainMetricMap.put(domain, metricLog);
            }

            String commonL7Protocol = value.getCommon_l7_protocol();
            int commonServerPort = value.getCommon_server_port();
            if (StringUtils.isNotBlank(commonL7Protocol)) {
                String key = commonL7Protocol + "-" + commonServerPort;
                Map<String, CnMetricLog> protocolMetricMap = accumulator.get(protocolMetricKey);
                CnMetricLog metricLog = protocolMetricMap.get(key);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.PROTOCOL_PORT);
                    initMetric(value, metricLog);
                    metricLog.setCommon_l7_protocol(commonL7Protocol);
                    metricLog.setCommon_server_port(commonServerPort);
                }
                protocolMetricMap.put(key, metricLog);
            }

            String client_Country = value.getClient_country_region();
            String client_Province = value.getClient_super_admin_area();
            String client_Region = value.getClient_admin_area();
            String client_zone = value.getClient_zone();
            String server_Country = value.getServer_country_region();
            String server_Province = value.getServer_super_admin_area();
            String server_Region = value.getServer_admin_area();
            String server_zone = value.getServer_zone();
            Long common_egress_link_id = value.getCommon_out_link_id();
            Long common_ingress_link_id = value.getCommon_in_link_id();
            String egress_link_direction = value.getOut_link_direction();
            String ingress_link_direction = value.getIn_link_direction();

            if (common_egress_link_id != null ||
                    common_ingress_link_id != null) {
                String key = client_Country
                        + "-" + client_Province
                        + "-" + client_Region
                        + "-" + client_zone
                        + "-" + server_Country
                        + "-" + server_Province
                        + "-" + server_Region
                        + "-" + server_zone
                        + "-" + common_egress_link_id
                        + "-" + common_ingress_link_id
                        + "-" + egress_link_direction
                        + "-" + ingress_link_direction;
                Map<String, CnMetricLog> linkMetricMap = accumulator.get(linkMetricKey);
                CnMetricLog metricLog = linkMetricMap.get(key);
                if (metricLog != null) {
                    updateMetric(value, metricLog);
                } else {
                    metricLog = new CnMetricLog(MetricKeyConfig.LINK);
                    initMetric(value, metricLog);
                    metricLog.setClient_country_region(client_Country);
                    metricLog.setClient_super_admin_area(client_Province);
                    metricLog.setClient_admin_area(client_Region);
                    metricLog.setClient_zone(value.getClient_zone());
                    metricLog.setServer_country_region(server_Country);
                    metricLog.setServer_super_admin_area(server_Province);
                    metricLog.setServer_admin_area(server_Region);
                    metricLog.setServer_zone(value.getServer_zone());
                    metricLog.setCommon_out_link_id(value.getCommon_out_link_id());
                    metricLog.setCommon_in_link_id(value.getCommon_in_link_id());
                    metricLog.setIn_link_direction(value.getIn_link_direction());
                    metricLog.setOut_link_direction(value.getOut_link_direction());
                }
                linkMetricMap.put(key, metricLog);
            }

            String subscriberId = value.getSubscriber_id();
            if (StringUtils.isNotBlank(subscriberId)) {
                Map<String, CnMetricLog> subscriberMetricMap = accumulator.get(subscriberKey);
                CnMetricLog cnMetricLog = subscriberMetricMap.get(subscriberId);
                if (cnMetricLog != null) {
                    updateMetric(value, cnMetricLog);
                } else {
                    cnMetricLog = new CnMetricLog(MetricKeyConfig.SUBSCRIBER);
                    initMetric(value, cnMetricLog);
                    cnMetricLog.setSubscriber_id(subscriberId);
                    cnMetricLog.setImei(value.getImei());
                    cnMetricLog.setImsi(value.getImsi());
                    cnMetricLog.setPhone_number(value.getPhone_number());
                    cnMetricLog.setApn(value.getApn());
                }
            }

            String appLabel = value.getCommon_app_label();
            if (StringUtils.isNotBlank(subscriberId) && StringUtils.isNotBlank(appLabel)) {
                String key = subscriberId + "-" + appLabel;
                Map<String, CnMetricLog> subscriberAppMetricMap = accumulator.get(subscriberAppKey);
                CnMetricLog cnMetricLog = subscriberAppMetricMap.get(key);
                if (cnMetricLog != null) {
                    updateMetric(value, cnMetricLog);
                } else {
                    cnMetricLog = new CnMetricLog(MetricKeyConfig.SUBSCRIBER_APP);
                    cnMetricLog.setSubscriber_id(subscriberId);
                    cnMetricLog.setCommon_app_label(appLabel);
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    private void initMetric(CnRecordLog cnRecordLog, CnMetricLog cnMetricLog) {
        cnMetricLog.setCommon_start_time(cnRecordLog.getCommon_start_time());
        cnMetricLog.setCommon_recv_time(cnRecordLog.getCommon_recv_time());
        cnMetricLog.setCommon_c2s_pkt_num(cnRecordLog.getCommon_c2s_pkt_num());
        cnMetricLog.setCommon_s2c_pkt_num(cnRecordLog.getCommon_s2c_pkt_num());
        cnMetricLog.setCommon_c2s_byte_num(cnRecordLog.getCommon_c2s_byte_num());
        cnMetricLog.setCommon_s2c_byte_num(cnRecordLog.getCommon_s2c_byte_num());
        cnMetricLog.setCommon_sessions(cnRecordLog.getCommon_sessions());

        String commonL4Protocol = cnRecordLog.getCommon_l4_protocol();
        if ("IPv4_TCP".equals(commonL4Protocol) || "IPv6_TCP".equals(commonL4Protocol)){
            cnMetricLog.setC2s_tcp_pkt_num(cnRecordLog.getCommon_c2s_pkt_num());
            cnMetricLog.setS2c_tcp_pkt_num(cnRecordLog.getCommon_s2c_pkt_num());
            cnMetricLog.setC2s_tcp_byte_num(cnRecordLog.getCommon_c2s_byte_num());
            cnMetricLog.setS2c_tcp_byte_num(cnRecordLog.getCommon_s2c_byte_num());
            cnMetricLog.setCommon_c2s_tcp_lostlen(cnRecordLog.getCommon_c2s_tcp_lostlen());
            cnMetricLog.setCommon_s2c_tcp_lostlen(cnRecordLog.getCommon_s2c_tcp_lostlen());
            cnMetricLog.setCommon_c2s_tcp_unorder_num(cnRecordLog.getCommon_c2s_tcp_unorder_num());
            cnMetricLog.setCommon_s2c_tcp_unorder_num(cnRecordLog.getCommon_s2c_tcp_unorder_num());
            cnMetricLog.setCommon_c2s_byte_retrans(cnRecordLog.getCommon_c2s_byte_retrans());
            cnMetricLog.setCommon_s2c_byte_retrans(cnRecordLog.getCommon_s2c_byte_retrans());
            cnMetricLog.setCommon_c2s_pkt_retrans(cnRecordLog.getCommon_c2s_pkt_retrans());
            cnMetricLog.setCommon_s2c_pkt_retrans(cnRecordLog.getCommon_s2c_pkt_retrans());
            cnMetricLog.setTcp_count(1L);
            cnMetricLog.setCommon_establish_latency_ms(cnRecordLog.getCommon_establish_latency_ms());

        }
        String commonL7Protocol = cnRecordLog.getCommon_l7_protocol();
        long httpResponseLatencyMs = cnRecordLog.getHttp_response_latency_ms();
        long sslConLatencyMs = cnRecordLog.getSsl_con_latency_ms();
        if ("HTTP".equals(commonL7Protocol) && httpResponseLatencyMs != 0) {
            cnMetricLog.setHttp_count(1L);
            cnMetricLog.setHttp_response_latency_ms(cnRecordLog.getHttp_response_latency_ms());
        } else if ("SSL".equals(commonL7Protocol) && sslConLatencyMs != 0) {
            cnMetricLog.setSsl_count(1L);
            cnMetricLog.setSsl_con_latency_ms(cnRecordLog.getSsl_con_latency_ms());
        }

        cnMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte());
        cnMetricLog.setTraffic_inbound_pkt(cnRecordLog.getTraffic_inbound_pkt());
        cnMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte());
        cnMetricLog.setTraffic_outbound_pkt(cnRecordLog.getTraffic_outbound_pkt());
        cnMetricLog.setTraffic_internal_byte(cnRecordLog.getTraffic_internal_byte());
        cnMetricLog.setTraffic_internal_pkt(cnRecordLog.getTraffic_internal_pkt());
        cnMetricLog.setTraffic_through_byte(cnRecordLog.getTraffic_through_byte());
        cnMetricLog.setTraffic_through_pkt(cnRecordLog.getTraffic_through_pkt());
    }

    private void initFlagMetric(CnRecordLog cnRecordLog, CnMetricLog cnMetricLog) {
        long flag = cnRecordLog.getCommon_flags();
        if ((flag & 0x00000001) == 0x00000001) {
            cnMetricLog.setAsymmetric_sessions(1L);
        }
        if ((flag & 0x00000002) == 0x00000002) {
            cnMetricLog.setBulky_sessions(1L);
        }
        if ((flag & 0x00000004) == 0x00000004) {
            cnMetricLog.setCbr_streaming_sessions(1L);
        }
        if ((flag & 0x00000020) == 0x00000020) {
            cnMetricLog.setDownload_sessions(1L);
        }
        if ((flag & 0x00000040) == 0x00000040) {
            cnMetricLog.setInteractive_sessions(1L);
        }
        if ((flag & 0x00000200) == 0x00000200) {
            cnMetricLog.setPseudo_unidirectional_sessions(1L);
        }
        if ((flag & 0x00000400) == 0x00000400) {
            cnMetricLog.setStreaming_sessions(1L);
        }
        if ((flag & 0x00000800) == 0x00000800) {
            cnMetricLog.setUnidirectional_sessions(1L);
        }
        if ((flag & 0x00001000) == 0x00001000) {
            cnMetricLog.setRandom_looking_sessions(1L);
        }
        if ((flag & 0x00008000) == 0x00008000) {
            cnMetricLog.setBidirectional_sessions(1L);
        }
    }

    private void updateMetric(CnRecordLog cnRecordLog, CnMetricLog cnMetricLog) {
        cnMetricLog.setCommon_c2s_pkt_num(cnRecordLog.getCommon_c2s_pkt_num() + cnMetricLog.getCommon_c2s_pkt_num());
        cnMetricLog.setCommon_s2c_pkt_num(cnRecordLog.getCommon_s2c_pkt_num() + cnMetricLog.getCommon_s2c_pkt_num());
        cnMetricLog.setCommon_c2s_byte_num(cnRecordLog.getCommon_c2s_byte_num() + cnMetricLog.getCommon_c2s_byte_num());
        cnMetricLog.setCommon_s2c_byte_num(cnRecordLog.getCommon_s2c_byte_num() + cnMetricLog.getCommon_s2c_byte_num());
        cnMetricLog.setCommon_sessions(cnRecordLog.getCommon_sessions() + cnMetricLog.getCommon_sessions());

        String commonL4Protocol = cnRecordLog.getCommon_l4_protocol();
        if ("IPv4_TCP".equals(commonL4Protocol) || "IPv6_TCP".equals(commonL4Protocol)){
            cnMetricLog.setC2s_tcp_pkt_num(cnRecordLog.getCommon_c2s_pkt_num() + cnMetricLog.getC2s_tcp_pkt_num());
            cnMetricLog.setS2c_tcp_pkt_num(cnRecordLog.getCommon_s2c_pkt_num() + cnMetricLog.getS2c_tcp_pkt_num());
            cnMetricLog.setC2s_tcp_byte_num(cnRecordLog.getCommon_c2s_byte_num() + cnMetricLog.getC2s_tcp_byte_num());
            cnMetricLog.setS2c_tcp_byte_num(cnRecordLog.getCommon_s2c_byte_num() + cnMetricLog.getS2c_tcp_byte_num());
            cnMetricLog.setCommon_c2s_tcp_lostlen(cnRecordLog.getCommon_c2s_tcp_lostlen() + cnMetricLog.getCommon_c2s_tcp_lostlen());
            cnMetricLog.setCommon_s2c_tcp_lostlen(cnRecordLog.getCommon_s2c_tcp_lostlen() + cnMetricLog.getCommon_s2c_tcp_lostlen());
            cnMetricLog.setCommon_c2s_tcp_unorder_num(cnRecordLog.getCommon_c2s_tcp_unorder_num() + cnMetricLog.getCommon_c2s_tcp_unorder_num());
            cnMetricLog.setCommon_s2c_tcp_unorder_num(cnRecordLog.getCommon_s2c_tcp_unorder_num() + cnMetricLog.getCommon_s2c_tcp_unorder_num());
            cnMetricLog.setCommon_c2s_byte_retrans(cnRecordLog.getCommon_c2s_byte_retrans() + cnMetricLog.getCommon_c2s_byte_retrans());
            cnMetricLog.setCommon_s2c_byte_retrans(cnRecordLog.getCommon_s2c_byte_retrans() + cnMetricLog.getCommon_s2c_byte_retrans());
            cnMetricLog.setCommon_c2s_pkt_retrans(cnRecordLog.getCommon_c2s_pkt_retrans() + cnMetricLog.getCommon_c2s_pkt_retrans());
            cnMetricLog.setCommon_s2c_pkt_retrans(cnRecordLog.getCommon_s2c_pkt_retrans() + cnMetricLog.getCommon_s2c_pkt_retrans());
            cnMetricLog.setTcp_count(cnMetricLog.getTcp_count() + 1L);
            cnMetricLog.setCommon_establish_latency_ms(cnRecordLog.getCommon_establish_latency_ms() + cnMetricLog.getCommon_establish_latency_ms());
        }
        String commonL7Protocol = cnRecordLog.getCommon_l7_protocol();
        long httpResponseLatencyMs = cnRecordLog.getHttp_response_latency_ms();
        long sslConLatencyMs = cnRecordLog.getSsl_con_latency_ms();
        if ("HTTP".equals(commonL7Protocol) && httpResponseLatencyMs != 0) {
            cnMetricLog.setHttp_count(cnMetricLog.getHttp_count() + 1L);
            cnMetricLog.setHttp_response_latency_ms(cnRecordLog.getHttp_response_latency_ms() + cnMetricLog.getHttp_response_latency_ms());
        } else if ("SSL".equals(commonL7Protocol) && sslConLatencyMs != 0) {
            cnMetricLog.setSsl_count(cnMetricLog.getSsl_count() + 1L);
            cnMetricLog.setSsl_con_latency_ms(cnRecordLog.getSsl_con_latency_ms() + cnMetricLog.getSsl_con_latency_ms());
        }

        cnMetricLog.setTraffic_inbound_byte(cnRecordLog.getTraffic_inbound_byte() + cnMetricLog.getTraffic_inbound_byte());
        cnMetricLog.setTraffic_inbound_pkt(cnRecordLog.getTraffic_inbound_pkt() + cnMetricLog.getTraffic_inbound_pkt());
        cnMetricLog.setTraffic_outbound_byte(cnRecordLog.getTraffic_outbound_byte() + cnMetricLog.getTraffic_outbound_byte());
        cnMetricLog.setTraffic_outbound_pkt(cnRecordLog.getTraffic_outbound_pkt() + cnMetricLog.getTraffic_outbound_pkt());
        cnMetricLog.setTraffic_internal_byte(cnRecordLog.getTraffic_internal_byte() + cnMetricLog.getTraffic_internal_byte());
        cnMetricLog.setTraffic_internal_pkt(cnRecordLog.getTraffic_internal_pkt() + cnMetricLog.getTraffic_internal_pkt());
        cnMetricLog.setTraffic_through_byte(cnRecordLog.getTraffic_through_byte() + cnMetricLog.getTraffic_through_byte());
        cnMetricLog.setTraffic_through_pkt(cnRecordLog.getTraffic_through_pkt() + cnMetricLog.getTraffic_through_pkt());
    }

    private void updateFlagMetric(CnRecordLog cnRecordLog, CnMetricLog cnMetricLog) {
        long flag = cnRecordLog.getCommon_flags();
        if ((flag & 0x00000001) == 0x00000001) {
            cnMetricLog.setAsymmetric_sessions(cnMetricLog.getAsymmetric_sessions() + 1L);
        }
        if ((flag & 0x00000002) == 0x00000002) {
            cnMetricLog.setBulky_sessions(cnMetricLog.getBulky_sessions() + 1L);
        }
        if ((flag & 0x00000004) == 0x00000004) {
            cnMetricLog.setCbr_streaming_sessions(cnMetricLog.getCbr_streaming_sessions() + 1L);
        }
        if ((flag & 0x00000020) == 0x00000020) {
            cnMetricLog.setDownload_sessions(cnMetricLog.getDownload_sessions() + 1L);
        }
        if ((flag & 0x00000040) == 0x00000040) {
            cnMetricLog.setInteractive_sessions(cnMetricLog.getInteractive_sessions() + 1L);
        }
        if ((flag & 0x00000200) == 0x00000200) {
            cnMetricLog.setPseudo_unidirectional_sessions(cnMetricLog.getPseudo_unidirectional_sessions() + 1L);
        }
        if ((flag & 0x00000400) == 0x00000400) {
            cnMetricLog.setStreaming_sessions(cnMetricLog.getStreaming_sessions() + 1L);
        }
        if ((flag & 0x00000800) == 0x00000800) {
            cnMetricLog.setUnidirectional_sessions(cnMetricLog.getUnidirectional_sessions() + 1L);
        }
        if ((flag & 0x00001000) == 0x00001000) {
            cnMetricLog.setRandom_looking_sessions(cnMetricLog.getRandom_looking_sessions() + 1L);
        }
        if ((flag & 0x00008000) == 0x00008000) {
            cnMetricLog.setBidirectional_sessions(cnMetricLog.getBidirectional_sessions() + 1L);
        }
    }
}
