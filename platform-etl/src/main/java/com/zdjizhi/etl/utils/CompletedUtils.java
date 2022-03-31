package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.Entity;
import com.zdjizhi.utils.FormatUtils;
import com.zdjizhi.utils.StringUtil;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/8
 * @ Description
 */
public class CompletedUtils {
    public static void getCompletedMessage(Entity entity, CnRecordLog recordLog){
        recordLog.setCommon_recv_time(entity.getCommon_end_time());
        recordLog.setCommon_direction(entity.getCommon_direction());
        recordLog.setCommon_schema_type(entity.getCommon_schema_type());
        recordLog.setCommon_client_ip(entity.getCommon_client_ip());
        recordLog.setCommon_server_ip(entity.getCommon_server_ip());
        recordLog.setCommon_client_port(entity.getCommon_client_port());
        recordLog.setCommon_server_port(entity.getCommon_server_port());
        recordLog.setCommon_app_label(entity.getCommon_app_label());
        recordLog.setCommon_l4_protocol(entity.getCommon_l4_protocol());
        recordLog.setCommon_l7_protocol(entity.getCommon_l7_protocol());
        recordLog.setCommon_isp(entity.getCommon_isp());
        recordLog.setCommon_egress_link_id(entity.getCommon_egress_link_id());
        recordLog.setCommon_ingress_link_id(entity.getCommon_ingress_link_id());
        recordLog.setCommon_stream_dir(entity.getCommon_stream_dir());
        recordLog.setHttp_host(entity.getHttp_host());
        recordLog.setSsl_sni(entity.getSsl_sni());
        recordLog.setHttp_url(entity.getHttp_url());
        recordLog.setHttp_cookie(entity.getHttp_cookie());
        recordLog.setHttp_referer(entity.getHttp_referer());
        recordLog.setHttp_user_agent(entity.getHttp_user_agent());
        recordLog.setHttp_request_line(entity.getHttp_request_line());
        recordLog.setHttp_response_line(entity.getHttp_response_line());
        recordLog.setHttp_status_code(getHttpStatusCode(entity));
        recordLog.setSsl_sni(entity.getSsl_sni());
        recordLog.setSsl_san(entity.getSsl_san());
        recordLog.setSsl_ja3_fingerprint(entity.getSsl_ja3_fingerprint());
        recordLog.setSsl_ja3_hash(entity.getSsl_ja3_hash());
        recordLog.setSsl_cert_issuer(entity.getSsl_cert_issuer());
        recordLog.setSsl_cert_subject(entity.getSsl_cert_subject());
        recordLog.setDns_qr(entity.getDns_qr());
        recordLog.setDns_opcode(entity.getDns_opcode());
        recordLog.setDns_aa(entity.getDns_aa());
        recordLog.setDns_rcode(entity.getDns_rcode());
        recordLog.setDns_qname(entity.getDns_qname());
        recordLog.setDns_qtype(entity.getDns_qtype());
        recordLog.setDns_qclass(entity.getDns_qclass());
        recordLog.setDns_sub(entity.getDns_sub());
        recordLog.setDns_rr(entity.getDns_rr());
        recordLog.setSsh_version(entity.getSsh_version());
        recordLog.setSsh_auth_success(entity.getSsh_auth_success());
        recordLog.setSsh_client_version(entity.getSsh_client_version());
        recordLog.setSsh_server_version(entity.getSsh_server_version());
        recordLog.setSsh_cipher_alg(entity.getSsh_cipher_alg());
        recordLog.setSsh_mac_alg(entity.getSsh_mac_alg());
        recordLog.setSsh_compression_alg(entity.getSsh_compression_alg());
        recordLog.setSsh_kex_alg(entity.getSsh_kex_alg());
        recordLog.setSsh_host_key_alg(entity.getSsh_host_key_alg());
        recordLog.setSsh_host_key(entity.getSsh_host_key());
        recordLog.setSsh_hassh(entity.getSsh_hassh());
        //domain
        if (StringUtil.isNotEmpty(entity.getHttp_host())){
            recordLog.setDomain(FormatUtils.getTopPrivateDomain(entity.getHttp_host()));
        }else {
            recordLog.setDomain(FormatUtils.getTopPrivateDomain(entity.getSsl_sni()));
        }
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
        recordLog.setDns_response_latency_ms(entity.getDns_response_latency_ms());

    }

    private static Integer getHttpStatusCode(Entity entity){
        int httpStatusCode = 0;
        String httpResponseLine = entity.getHttp_response_line();
        if (StringUtil.isNotBlank(httpResponseLine)){
            String[] split = httpResponseLine.split(" ");
            if (split.length >= 2){
                httpStatusCode = Integer.parseInt(split[1]);
            }
        }
        return httpStatusCode;
    }

}
