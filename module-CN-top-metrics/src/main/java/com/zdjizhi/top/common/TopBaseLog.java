package com.zdjizhi.top.common;

import com.zdjizhi.base.common.CnRecordLog;

import java.io.Serializable;
import java.util.Objects;

public class TopBaseLog implements Serializable {
    private String common_client_ip;
    private String common_server_ip;
    private String domain;
    private String common_app_label;

    private long common_recv_time;
    private String group_by;
    private String order_by;
    private long time_granularity;

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
    private double packet_loss;
    private double packet_retrans;

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public String getCommon_server_ip() {
        return common_server_ip;
    }

    public void setCommon_server_ip(String common_server_ip) {
        this.common_server_ip = common_server_ip;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getGroup_by() {
        return group_by;
    }

    public void setGroup_by(String group_by) {
        this.group_by = group_by;
    }

    public String getOrder_by() {
        return order_by;
    }

    public void setOrder_by(String order_by) {
        this.order_by = order_by;
    }

    public long getTime_granularity() {
        return time_granularity;
    }

    public void setTime_granularity(long time_granularity) {
        this.time_granularity = time_granularity;
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

    public double getPacket_loss() {
        return packet_loss;
    }

    public void setPacket_loss(double packet_loss) {
        this.packet_loss = packet_loss;
    }

    public double getPacket_retrans() {
        return packet_retrans;
    }

    public void setPacket_retrans(double packet_retrans) {
        this.packet_retrans = packet_retrans;
    }

    public Number get(String fieldName) {
        switch (fieldName) {
            case "sessions":
                return getCommon_sessions();
            case "packets":
                return getCommon_c2s_pkt_num() + getCommon_s2c_pkt_num();
            case "bytes":
                return getCommon_c2s_byte_num() + getCommon_s2c_byte_num();
            case "establish_latency":
                return getCommon_establish_latency_ms();
            case "http_response_latency":
                return getHttp_response_latency_ms();
            case "ssl_con_latency":
                return getSsl_con_latency_ms();
            case "packet_loss":
                return getPacket_loss();
            case "packet_retrans":
                return getPacket_retrans();
            default:
                return 0L;
        }
    }

    public void set(String fieldName, CnRecordLog value) {
        switch (fieldName) {
            case "common_client_ip":
                setCommon_client_ip(value.getCommon_client_ip());
                break;
            case "common_server_ip":
                setCommon_server_ip(value.getCommon_server_ip());
                break;
            case "domain":
                setDomain(value.getDomain());
                break;
            case "common_app_label":
                setCommon_app_label(value.getCommon_app_label());
                break;
            default:
                break;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopBaseLog)) {
            return false;
        }
        TopBaseLog that = (TopBaseLog) o;
        return getCommon_recv_time() == that.getCommon_recv_time() &&
                getTime_granularity() == that.getTime_granularity() &&
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
                Double.compare(that.getPacket_loss(), getPacket_loss()) == 0 &&
                Double.compare(that.getPacket_retrans(), getPacket_retrans()) == 0 &&
                Objects.equals(getCommon_client_ip(), that.getCommon_client_ip()) &&
                Objects.equals(getCommon_server_ip(), that.getCommon_server_ip()) &&
                Objects.equals(getDomain(), that.getDomain()) &&
                Objects.equals(getCommon_app_label(), that.getCommon_app_label()) &&
                Objects.equals(getGroup_by(), that.getGroup_by()) &&
                Objects.equals(getOrder_by(), that.getOrder_by());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCommon_client_ip(), getCommon_server_ip(), getDomain(), getCommon_app_label(), getCommon_recv_time(), getGroup_by(), getOrder_by(), getTime_granularity(), getCommon_c2s_pkt_num(), getCommon_c2s_byte_num(), getCommon_s2c_pkt_num(), getCommon_s2c_byte_num(), getCommon_sessions(), getCommon_c2s_tcp_lostlen(), getCommon_s2c_tcp_lostlen(), getCommon_c2s_tcp_unorder_num(), getCommon_s2c_tcp_unorder_num(), getCommon_c2s_byte_retrans(), getCommon_s2c_byte_retrans(), getCommon_c2s_pkt_retrans(), getCommon_s2c_pkt_retrans(), getCommon_establish_latency_ms(), getHttp_response_latency_ms(), getSsl_con_latency_ms(), getPacket_loss(), getPacket_retrans());
    }

    @Override
    public String toString() {
        return "TopBaseLog{" +
                "common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
                ", domain='" + domain + '\'' +
                ", common_app_label='" + common_app_label + '\'' +
                ", common_recv_time=" + common_recv_time +
                ", group_by='" + group_by + '\'' +
                ", order_by='" + order_by + '\'' +
                ", time_granularity=" + time_granularity +
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
                ", packet_loss=" + packet_loss +
                ", packet_retrans=" + packet_retrans +
                '}';
    }
}
