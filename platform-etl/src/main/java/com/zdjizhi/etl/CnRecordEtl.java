package com.zdjizhi.etl;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

public class CnRecordEtl implements Schedule {
    private static final Logger LOG = LoggerFactory.getLogger(CnRecordEtl.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();


    public static SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return createCnRecordSource();
    }

    @Override
    public void schedule() throws Exception {
        getCnRecordSource()
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.CN_RECORD_TOPIC))
                .setParallelism(CommonConfig.CN_RECORD_PARALLELISM);
    }

    private static SingleOutputStreamOperator<CnRecordLog> createCnRecordSource() {
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                property);

        DataStreamSource<String> source = Source.getSingleSource(sourceConfig);

        SingleOutputStreamOperator<Entity> input = source.map((MapFunction<String, Entity>) message -> {
            Entity entity = new Entity();
            try {
                entity = jsonMapperInstance.fromJson(message, Entity.class);
                String commonAppLabel = entity.getCommon_app_label();
                if (StringUtil.isNotEmpty(commonAppLabel)) {
                    if (commonAppLabel.startsWith("05")) {
                        entity.setCommon_app_label("");
                    }
                }
                if (entity.getCommon_ingress_link_id() > 2050 || entity.getCommon_egress_link_id() > 2050) {
                    entity.setCommon_ingress_link_id(0);
                    entity.setCommon_egress_link_id(0);
                }
            } catch (Exception e) {
                LOG.error("Entity Parsing ERROR");
                entity.setIfError(1);
            }
            return entity;
        }).filter((FilterFunction<Entity>) entity -> entity.getIfError() != 1);

        return input.map((MapFunction<Entity, CnRecordLog>) entity -> {
            CnRecordLog recordLog = new CnRecordLog();
            try {
                recordLog.setCommon_recv_time(entity.getCommon_recv_time());
                //common_direction
                recordLog.setCommon_direction(entity.getCommon_direction());
                recordLog.setCommon_schema_type(entity.getCommon_schema_type());
                recordLog.setCommon_client_ip(entity.getCommon_client_ip());
                recordLog.setCommon_server_ip(entity.getCommon_server_ip());
                recordLog.setCommon_client_port(entity.getCommon_client_port());
                recordLog.setCommon_server_port(entity.getCommon_server_port());
                recordLog.setCommon_app_label(entity.getCommon_app_label());
                recordLog.setCommon_l4_protocol(entity.getCommon_l4_protocol());
                //common_l7_protocol
                recordLog.setCommon_l7_protocol(entity.getCommon_l7_protocol());
                //common_isp
                recordLog.setCommon_isp(entity.getCommon_isp());
                recordLog.setCommon_egress_link_id(entity.getCommon_egress_link_id());
                recordLog.setCommon_ingress_link_id(entity.getCommon_ingress_link_id());
                recordLog.setHttp_host(entity.getHttp_host());
                recordLog.setSsl_sni(entity.getSsl_sni());
                //http_url
                recordLog.setHttp_url(entity.getHttp_url());
                //http_cookie
                recordLog.setHttp_cookie(entity.getHttp_cookie());
                //http_referer
                recordLog.setHttp_referer(entity.getHttp_referer());
                //http_user_agent
                recordLog.setHttp_user_agent(entity.getHttp_user_agent());

                recordLog.setHttp_status_code(entity.getHttp_status_code());
                //ssl_sni
                recordLog.setSsl_sni(entity.getSsl_sni());
                //ssl_san
                recordLog.setSsl_san(entity.getSsl_san());
                //ssl_ja3_fingerprint
                recordLog.setSsl_ja3_fingerprint(entity.getSsl_ja3_fingerprint());
                //ssl_ja3_hash
                recordLog.setSsl_ja3_hash(entity.getSsl_ja3_hash());
                //ssl_cert_issuer
                recordLog.setSsl_cert_issuer(entity.getSsl_cert_issuer());
                //ssl_cert_subject
                recordLog.setSsl_cert_subject(entity.getSsl_cert_subject());
                //dns_qr
                recordLog.setDns_qr(entity.getDns_qr());
                //dns_opcode
                recordLog.setDns_opcode(entity.getDns_opcode());
                //dns_aa
                recordLog.setDns_aa(entity.getDns_aa());
                //dns_rcode
                recordLog.setDns_rcode(entity.getDns_rcode());
                //dns_qname
                recordLog.setDns_qname(entity.getDns_qname());
                //dns_qtype
                recordLog.setDns_qtype(entity.getDns_qtype());
                //dns_qclass
                recordLog.setDns_qclass(entity.getDns_qclass());
                //dns_sub
                recordLog.setDns_sub(entity.getDns_sub());
                //dns_rr
                recordLog.setDns_rr(entity.getDns_rr());
                //ssh_version
                recordLog.setSsh_version(entity.getSsh_version());
                //ssh_auth_success
                recordLog.setSsh_auth_success(entity.getSsh_auth_success());
                //ssh_client_version
                recordLog.setSsh_client_version(entity.getSsh_client_version());
                //ssh_server_version
                recordLog.setSsh_server_version(entity.getSsh_server_version());
                //ssh_cipher_alg
                recordLog.setSsh_cipher_alg(entity.getSsh_cipher_alg());
                //ssh_mac_alg
                recordLog.setSsh_mac_alg(entity.getSsh_mac_alg());
                //ssh_compression_alg
                recordLog.setSsh_compression_alg(entity.getSsh_compression_alg());
                //ssh_kex_alg
                recordLog.setSsh_kex_alg(entity.getSsh_kex_alg());
                //ssh_host_key_alg
                recordLog.setSsh_host_key_alg(entity.getSsh_host_key_alg());
                //ssh_host_key
                recordLog.setSsh_host_key(entity.getSsh_host_key());
                //ssh_hassh
                recordLog.setSsh_hassh(entity.getSsh_hassh());
                //domain
//            if (entity.getHttp_host().equals("")){
//                String[] subDomain = entity.getSsl_sni().split(".com");
//                recordLog.setDomain(subDomain[0]);
//                System.out.println("domain:"+recordLog.getDomain());
//            }else {
//                String[] subDomain = entity.getHttp_host().split(".cn");
//                recordLog.setDomain(subDomain[0]);
//                System.out.println("domain:"+recordLog.getDomain());
//            }
                recordLog.setDomain(entity.getHttp_domain());
                recordLog.setCommon_c2s_pkt_num(entity.getCommon_c2s_pkt_num());
                recordLog.setCommon_c2s_byte_num(entity.getCommon_c2s_byte_num());
                recordLog.setCommon_s2c_pkt_num(entity.getCommon_s2c_pkt_num());
                recordLog.setCommon_s2c_byte_num(entity.getCommon_s2c_byte_num());
                recordLog.setCommon_sessions(entity.getCommon_sessions());
                recordLog.setCommon_c2s_tcp_lostlen(entity.getCommon_c2s_tcp_lostlen());
                recordLog.setCommon_s2c_tcp_lostlen(entity.getCommon_s2c_tcp_lostlen());
                recordLog.setCommon_c2s_tcp_unorder_num(entity.getCommon_c2s_tcp_unorder_num());
                recordLog.setCommon_s2c_tcp_unorder_num(entity.getCommon_s2c_tcp_unorder_num());
                recordLog.setCommon_c2s_byte_retrans(entity.getCommon_c2s_byte_retrans());
                recordLog.setCommon_s2c_byte_retrans(entity.getCommon_s2c_byte_retrans());
                recordLog.setCommon_c2s_pkt_retrans(entity.getCommon_c2s_pkt_retrans());
                recordLog.setCommon_s2c_pkt_retrans(entity.getCommon_s2c_pkt_retrans());
                recordLog.setCommon_establish_latency_ms(entity.getCommon_establish_latency_ms());
                recordLog.setHttp_response_latency_ms(entity.getHttp_response_latency_ms());
                recordLog.setSsl_con_latency_ms(entity.getSsl_con_latency_ms());
                //dns_response_latency_ms
                recordLog.setDns_response_latency_ms(entity.getDns_response_latency_ms());
                //dns_protocol
                if ("DNS".equals(entity.getCommon_l7_protocol())){
                    if (entity.getCommon_server_port()==443 && dnsmap.get(recordLog.getCommon_server_ip()).getDoh_support()==1){
                        recordLog.setDns_protocol("doh");
                    }else if (entity.getCommon_server_port()==853 && dnsmap.get(recordLog.getCommon_server_ip()).getDot_support()==1){
                        recordLog.setDns_protocol("dot");
                    }else if (entity.getDns_sub()==1){
                        recordLog.setDns_protocol("dnssec");
                    }else{
                        recordLog.setDns_protocol("dns");
                    }
                }else {
                    recordLog.setDns_protocol("");
                }

                IPUtils.getMmdbMessage(recordLog);
                FcUtils.getWebSketchMessage(recordLog);
                FcUtils.getIcpMessage(recordLog);
                //todo whois
                WhoisUtils.getWhoisSketchMessage(recordLog);
                //todo dns
//            DnsServerUtils.getWebSktMessage(recordLog);
                getWebSktMessage(recordLog);
                AppUtils.getAppSketchMessage(recordLog);

            }catch (Exception e){
                LOG.error("{}\n ETL解析错误\n",entity.toString(),e);
            }
            return recordLog;
        });
    }

    private static HashMap<String, CnRecordLog> dnsmap = readServerCsv(CommonConfig.DNS_PATH);


    private static HashMap<String, CnRecordLog> readServerCsv(String path) {
        HashMap<String , CnRecordLog> dnsmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[1];
                ArrayList<String> serverRole=new ArrayList<>();
                if (!key.startsWith("#")){
                    CnRecordLog catalog = new CnRecordLog();
                    catalog.setCommon_server_ip(key);
                    serverRole.add(strs[6]);
                    catalog.setDns_server_role(serverRole);
                    catalog.setDns_server_org(strs[7]);
                    catalog.setDns_server_software(strs[9]);
                    catalog.setDns_server_os(strs[8]);
                    dnsmap.put(key, catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return  dnsmap;
    }




    public static void getWebSktMessage(CnRecordLog cnRecordLog){
        try{
            if (dnsmap.containsKey(cnRecordLog.getCommon_server_ip())){
                cnRecordLog.setDns_server_role(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_role());
                cnRecordLog.setDns_server_org(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_org());
                cnRecordLog.setDns_server_os(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_os());
                cnRecordLog.setDns_server_software(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_software());
            }
        }catch (Exception e) {
            LOG.error(e.toString());
        }
    }

}
