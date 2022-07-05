package com.zdjizhi.top.common;

import java.io.Serializable;
import java.util.Objects;

public class MetricMapLog implements Serializable {

    private String server_country;
    private String client_country;
    private String server_province;
    private String client_province;
    private String server_region;
    private String client_region;
    private String common_schema_type;
    private String common_l4_protocol;
    private long common_recv_time;
    private long common_c2s_pkt_num;
    private long common_c2s_byte_num;
    private long common_s2c_pkt_num;
    private long common_s2c_byte_num;
    private long common_sessions;
    private long common_c2s_tcp_lostlen;
    private long common_s2c_tcp_lostlen;
    private long common_c2s_tcp_unorder_num;
    private long common_s2c_tcp_unorder_num;
    private long common_c2s_byte_retrans;
    private long common_s2c_byte_retrans;
    private long common_c2s_pkt_retrans;
    private long common_s2c_pkt_retrans;
    private long common_establish_latency_ms;
    private long http_response_latency_ms;
    private long ssl_con_latency_ms;

    public String getServer_country() {
        return server_country;
    }

    public void setServer_country(String server_country) {
        this.server_country = server_country;
    }

    public String getClient_country() {
        return client_country;
    }

    public void setClient_country(String client_country) {
        this.client_country = client_country;
    }

    public String getServer_province() {
        return server_province;
    }

    public void setServer_province(String server_province) {
        this.server_province = server_province;
    }

    public String getClient_province() {
        return client_province;
    }

    public void setClient_province(String client_province) {
        this.client_province = client_province;
    }

    public String getServer_region() {
        return server_region;
    }

    public void setServer_region(String server_region) {
        this.server_region = server_region;
    }

    public String getClient_region() {
        return client_region;
    }

    public void setClient_region(String client_region) {
        this.client_region = client_region;
    }

    public String getCommon_schema_type() {
        return common_schema_type;
    }

    public void setCommon_schema_type(String common_schema_type) {
        this.common_schema_type = common_schema_type;
    }

    public String getCommon_l4_protocol() {
        return common_l4_protocol;
    }

    public void setCommon_l4_protocol(String common_l4_protocol) {
        this.common_l4_protocol = common_l4_protocol;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public long getCommon_c2s_pkt_num() {
        return common_c2s_pkt_num;
    }

    public void setCommon_c2s_pkt_num(long common_c2s_pkt_num) {
        this.common_c2s_pkt_num = common_c2s_pkt_num;
    }

    public long getCommon_c2s_byte_num() {
        return common_c2s_byte_num;
    }

    public void setCommon_c2s_byte_num(long common_c2s_byte_num) {
        this.common_c2s_byte_num = common_c2s_byte_num;
    }

    public long getCommon_s2c_pkt_num() {
        return common_s2c_pkt_num;
    }

    public void setCommon_s2c_pkt_num(long common_s2c_pkt_num) {
        this.common_s2c_pkt_num = common_s2c_pkt_num;
    }

    public long getCommon_s2c_byte_num() {
        return common_s2c_byte_num;
    }

    public void setCommon_s2c_byte_num(long common_s2c_byte_num) {
        this.common_s2c_byte_num = common_s2c_byte_num;
    }

    public long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(long common_sessions) {
        this.common_sessions = common_sessions;
    }

    public long getCommon_c2s_tcp_lostlen() {
        return common_c2s_tcp_lostlen;
    }

    public void setCommon_c2s_tcp_lostlen(long common_c2s_tcp_lostlen) {
        this.common_c2s_tcp_lostlen = common_c2s_tcp_lostlen;
    }

    public long getCommon_s2c_tcp_lostlen() {
        return common_s2c_tcp_lostlen;
    }

    public void setCommon_s2c_tcp_lostlen(long common_s2c_tcp_lostlen) {
        this.common_s2c_tcp_lostlen = common_s2c_tcp_lostlen;
    }

    public long getCommon_c2s_tcp_unorder_num() {
        return common_c2s_tcp_unorder_num;
    }

    public void setCommon_c2s_tcp_unorder_num(long common_c2s_tcp_unorder_num) {
        this.common_c2s_tcp_unorder_num = common_c2s_tcp_unorder_num;
    }

    public long getCommon_s2c_tcp_unorder_num() {
        return common_s2c_tcp_unorder_num;
    }

    public void setCommon_s2c_tcp_unorder_num(long common_s2c_tcp_unorder_num) {
        this.common_s2c_tcp_unorder_num = common_s2c_tcp_unorder_num;
    }

    public long getCommon_c2s_byte_retrans() {
        return common_c2s_byte_retrans;
    }

    public void setCommon_c2s_byte_retrans(long common_c2s_byte_retrans) {
        this.common_c2s_byte_retrans = common_c2s_byte_retrans;
    }

    public long getCommon_s2c_byte_retrans() {
        return common_s2c_byte_retrans;
    }

    public void setCommon_s2c_byte_retrans(long common_s2c_byte_retrans) {
        this.common_s2c_byte_retrans = common_s2c_byte_retrans;
    }

    public long getCommon_c2s_pkt_retrans() {
        return common_c2s_pkt_retrans;
    }

    public void setCommon_c2s_pkt_retrans(long common_c2s_pkt_retrans) {
        this.common_c2s_pkt_retrans = common_c2s_pkt_retrans;
    }

    public long getCommon_s2c_pkt_retrans() {
        return common_s2c_pkt_retrans;
    }

    public void setCommon_s2c_pkt_retrans(long common_s2c_pkt_retrans) {
        this.common_s2c_pkt_retrans = common_s2c_pkt_retrans;
    }

    public long getCommon_establish_latency_ms() {
        return common_establish_latency_ms;
    }

    public void setCommon_establish_latency_ms(long common_establish_latency_ms) {
        this.common_establish_latency_ms = common_establish_latency_ms;
    }

    public long getHttp_response_latency_ms() {
        return http_response_latency_ms;
    }

    public void setHttp_response_latency_ms(long http_response_latency_ms) {
        this.http_response_latency_ms = http_response_latency_ms;
    }

    public long getSsl_con_latency_ms() {
        return ssl_con_latency_ms;
    }

    public void setSsl_con_latency_ms(long ssl_con_latency_ms) {
        this.ssl_con_latency_ms = ssl_con_latency_ms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricMapLog)) {
            return false;
        }
        MetricMapLog that = (MetricMapLog) o;
        return getCommon_recv_time() == that.getCommon_recv_time() &&
                getCommon_c2s_pkt_num() == that.getCommon_c2s_pkt_num() &&
                getCommon_c2s_byte_num() == that.getCommon_c2s_byte_num() &&
                getCommon_s2c_pkt_num() == that.getCommon_s2c_pkt_num() &&
                getCommon_s2c_byte_num() == that.getCommon_s2c_byte_num() &&
                getCommon_sessions() == that.getCommon_sessions() &&
                getCommon_c2s_tcp_lostlen() == that.getCommon_c2s_tcp_lostlen() &&
                getCommon_s2c_tcp_lostlen() == that.getCommon_s2c_tcp_lostlen() &&
                getCommon_c2s_tcp_unorder_num() == that.getCommon_c2s_tcp_unorder_num() &&
                getCommon_s2c_tcp_unorder_num() == that.getCommon_s2c_tcp_unorder_num() &&
                getCommon_c2s_byte_retrans() == that.getCommon_c2s_byte_retrans() &&
                getCommon_s2c_byte_retrans() == that.getCommon_s2c_byte_retrans() &&
                getCommon_c2s_pkt_retrans() == that.getCommon_c2s_pkt_retrans() &&
                getCommon_s2c_pkt_retrans() == that.getCommon_s2c_pkt_retrans() &&
                getCommon_establish_latency_ms() == that.getCommon_establish_latency_ms() &&
                getHttp_response_latency_ms() == that.getHttp_response_latency_ms() &&
                getSsl_con_latency_ms() == that.getSsl_con_latency_ms() &&
                Objects.equals(getServer_country(), that.getServer_country()) &&
                Objects.equals(getClient_country(), that.getClient_country()) &&
                Objects.equals(getServer_province(), that.getServer_province()) &&
                Objects.equals(getClient_province(), that.getClient_province()) &&
                Objects.equals(getServer_region(), that.getServer_region()) &&
                Objects.equals(getClient_region(), that.getClient_region()) &&
                Objects.equals(getCommon_schema_type(), that.getCommon_schema_type()) &&
                Objects.equals(getCommon_l4_protocol(), that.getCommon_l4_protocol());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getServer_country(), getClient_country(), getServer_province(), getClient_province(), getServer_region(), getClient_region(), getCommon_schema_type(), getCommon_l4_protocol(), getCommon_recv_time(), getCommon_c2s_pkt_num(), getCommon_c2s_byte_num(), getCommon_s2c_pkt_num(), getCommon_s2c_byte_num(), getCommon_sessions(), getCommon_c2s_tcp_lostlen(), getCommon_s2c_tcp_lostlen(), getCommon_c2s_tcp_unorder_num(), getCommon_s2c_tcp_unorder_num(), getCommon_c2s_byte_retrans(), getCommon_s2c_byte_retrans(), getCommon_c2s_pkt_retrans(), getCommon_s2c_pkt_retrans(), getCommon_establish_latency_ms(), getHttp_response_latency_ms(), getSsl_con_latency_ms());
    }

    @Override
    public String toString() {
        return "MetricMapLog{" +
                "server_country='" + server_country + '\'' +
                ", client_country='" + client_country + '\'' +
                ", server_province='" + server_province + '\'' +
                ", client_province='" + client_province + '\'' +
                ", server_region='" + server_region + '\'' +
                ", client_region='" + client_region + '\'' +
                ", common_schema_type='" + common_schema_type + '\'' +
                ", common_l4_protocol='" + common_l4_protocol + '\'' +
                ", common_recv_time=" + common_recv_time +
                ", common_c2s_pkt_num=" + common_c2s_pkt_num +
                ", common_c2s_byte_num=" + common_c2s_byte_num +
                ", common_s2c_pkt_num=" + common_s2c_pkt_num +
                ", common_s2c_byte_num=" + common_s2c_byte_num +
                ", common_sessions=" + common_sessions +
                ", common_c2s_tcp_lostlen=" + common_c2s_tcp_lostlen +
                ", common_s2c_tcp_lostlen=" + common_s2c_tcp_lostlen +
                ", common_c2s_tcp_unorder_num=" + common_c2s_tcp_unorder_num +
                ", common_s2c_tcp_unorder_num=" + common_s2c_tcp_unorder_num +
                ", common_c2s_byte_retrans=" + common_c2s_byte_retrans +
                ", common_s2c_byte_retrans=" + common_s2c_byte_retrans +
                ", common_c2s_pkt_retrans=" + common_c2s_pkt_retrans +
                ", common_s2c_pkt_retrans=" + common_s2c_pkt_retrans +
                ", common_establish_latency_ms=" + common_establish_latency_ms +
                ", http_response_latency_ms=" + http_response_latency_ms +
                ", ssl_con_latency_ms=" + ssl_con_latency_ms +
                '}';
    }
}
