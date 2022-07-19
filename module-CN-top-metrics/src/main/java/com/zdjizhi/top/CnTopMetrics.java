package com.zdjizhi.top;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.top.common.MetricMapLog;
import com.zdjizhi.top.common.TopBaseLog;
import com.zdjizhi.top.operator.PreAggregationProcess;
import com.zdjizhi.top.operator.TopMetricAggregate;
import com.zdjizhi.top.operator.TopMetricProcessV2;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.util.Collector;

import java.util.Objects;

import static com.zdjizhi.base.utils.FlinkEnvironmentUtils.tableEnvironment;

public class CnTopMetrics implements Schedule {
    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;

        if (com.zdjizhi.top.common.CommonConfig.TOP_METRIC_USE_SQL_TYPE == 0) {
            Schema schema = Schema.newBuilder()
                    .columnByExpression("stat_time", "TO_TIMESTAMP(FROM_UNIXTIME(common_recv_time))")
                    .watermark("stat_time", "stat_time")
                    .build();
            Table table = tableEnvironment.fromDataStream(source, schema);
            tableEnvironment.createTemporaryView("base", table);

            tableEnvironment.executeSql(getClientIpViewddlSql());
            tableEnvironment.executeSql(getServerIpViewddlSql());
            tableEnvironment.executeSql(getDomainViewddlSql());
            tableEnvironment.executeSql(getAppLabelViewddlSql());
            tableEnvironment.executeSql(getCountryCityViewDdlSql());

            Table clientIpTable;
            Table serverIpTable;
            Table domainTable;
            Table appLabelTable;
            Table countryCityTable;

            if (com.zdjizhi.top.common.CommonConfig.TOP_METRIC_ALL_RESULT_TYPE == 0) {
                clientIpTable = tableEnvironment.sqlQuery(getInsertClientIpSql2());
                serverIpTable = tableEnvironment.sqlQuery(getInsertServerIpSql2());
                domainTable = tableEnvironment.sqlQuery(getInsertDomainSql2());
                appLabelTable = tableEnvironment.sqlQuery(getInsertAppLabelSql2());
                countryCityTable = tableEnvironment.sqlQuery(getCountryCitySql());
            } else {
                clientIpTable = tableEnvironment.sqlQuery(getInsertClientIpSql());
                serverIpTable = tableEnvironment.sqlQuery(getInsertServerIpSql());
                domainTable = tableEnvironment.sqlQuery(getInsertDomainSql());
                appLabelTable = tableEnvironment.sqlQuery(getInsertAppLabelSql());
                countryCityTable = tableEnvironment.sqlQuery(getCountryCitySql());
            }

            DataStream<Tuple2<Boolean, TopBaseLog>> clientIpDataStream = tableEnvironment.toRetractStream(clientIpTable, TopBaseLog.class);
            DataStream<Tuple2<Boolean, TopBaseLog>> serverIpDataStream = tableEnvironment.toRetractStream(serverIpTable, TopBaseLog.class);
            DataStream<Tuple2<Boolean, TopBaseLog>> domainDataStream = tableEnvironment.toRetractStream(domainTable, TopBaseLog.class);
            DataStream<Tuple2<Boolean, TopBaseLog>> appLabelDataStream = tableEnvironment.toRetractStream(appLabelTable, TopBaseLog.class);
            DataStream<Tuple2<Boolean, MetricMapLog>> countryCityDataStream = tableEnvironment.toRetractStream(countryCityTable, MetricMapLog.class);

            clientIpDataStream.filter(log -> log.f0 && log.f1 != null).map(log -> JsonMapper.toJsonString(log.f1))
                    .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC)).setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

            serverIpDataStream.filter(log -> log.f0 && log.f1 != null).map(log -> JsonMapper.toJsonString(log.f1))
                    .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC)).setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

            domainDataStream.filter(log -> log.f0 && log.f1 != null).map(log -> JsonMapper.toJsonString(log.f1))
                    .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC)).setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

            appLabelDataStream.filter(log -> log.f0 && log.f1 != null).map(log -> JsonMapper.toJsonString(log.f1))
                    .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC)).setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

            countryCityDataStream.filter(log -> log.f0 && log.f1 != null).map(log -> JsonMapper.toJsonString(log.f1))
                    .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_COUNTRY_CITY_TOPIC)).setParallelism(CommonConfig.TSG_BASE_COUNTRY_CITY_PARALLELISM);

        } else {
            if (com.zdjizhi.top.common.CommonConfig.TOP_METRIC_ALL_RESULT_TYPE == 0) {
                clientIpMetricsProcess2(source);
                serverIpMetricsProcess2(source);
                domainMetricsProcess2(source);
                appLableMetricsProcess2(source);
                countryCityMetrics(source);
            } else {
                clientIpMetricsProcess(source);
                serverIpMetricsProcess(source);
                domainMetricsProcess(source);
                appLableMetricsProcess(source);
                countryCityMetrics(source);
            }
        }
    }

    private String getClientIpViewddlSql() {
        return "CREATE VIEW top_base_view_client as\n" +
                "SELECT\n" +
                "\tcommon_client_ip as common_client_ip,\n" +
                "\tUNIX_TIMESTAMP(CAST(TUMBLE_END(stat_time,INTERVAL '1' MINUTE) as VARCHAR)) as common_recv_time,\n" +
                "\tsum(common_c2s_pkt_num) as common_c2s_pkt_num,\n" +
                "\tsum(common_c2s_byte_num) as common_c2s_byte_num,\n" +
                "\tsum(common_s2c_pkt_num) as common_s2c_pkt_num,\n" +
                "\tsum(common_s2c_byte_num) as common_s2c_byte_num,\n" +
                "\tsum(common_sessions) as common_sessions,\n" +
                "\tsum(common_c2s_tcp_lostlen) as common_c2s_tcp_lostlen,\n" +
                "\tsum(common_s2c_tcp_lostlen) as common_s2c_tcp_lostlen,\n" +
                "\tsum(common_c2s_tcp_unorder_num) as common_c2s_tcp_unorder_num,\n" +
                "\tsum(common_s2c_tcp_unorder_num) as common_s2c_tcp_unorder_num,\n" +
                "\tsum(common_c2s_byte_retrans) as common_c2s_byte_retrans,\n" +
                "\tsum(common_s2c_byte_retrans) as common_s2c_byte_retrans,\n" +
                "\tsum(common_c2s_pkt_retrans) as common_c2s_pkt_retrans,\n" +
                "\tsum(common_s2c_pkt_retrans) as common_s2c_pkt_retrans,\n" +
                "\tavg(common_establish_latency_ms) as common_establish_latency_ms,\n" +
                "\tavg(http_response_latency_ms) as http_response_latency_ms,\n" +
                "\tavg(ssl_con_latency_ms) as ssl_con_latency_ms,\n" +
                "\tROUND(AVG((cast ((common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))/(cast ((common_c2s_byte_num+common_s2c_byte_num+common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))),4) as packet_loss,\n" +
                "        ROUND(AVG((cast ((common_c2s_pkt_retrans+common_s2c_pkt_retrans) as double))/(cast ((common_c2s_pkt_num+common_s2c_pkt_num) as double))),4) as packet_retrans\n" +
                "FROM\n" +
                "\tbase\n" +
                "group by\n" +
                "        common_client_ip,\n" +
                "\tTUMBLE(stat_time,INTERVAL '1' MINUTE)";
    }

    private String getServerIpViewddlSql() {
        return "CREATE VIEW top_base_view_server as\n" +
                "SELECT\n" +
                "\tcommon_server_ip as common_server_ip,\n" +
                "\tUNIX_TIMESTAMP(CAST(TUMBLE_END(stat_time,INTERVAL '1' MINUTE) as VARCHAR)) as common_recv_time,\n" +
                "\tsum(common_c2s_pkt_num) as common_c2s_pkt_num,\n" +
                "\tsum(common_c2s_byte_num) as common_c2s_byte_num,\n" +
                "\tsum(common_s2c_pkt_num) as common_s2c_pkt_num,\n" +
                "\tsum(common_s2c_byte_num) as common_s2c_byte_num,\n" +
                "\tsum(common_sessions) as common_sessions,\n" +
                "\tsum(common_c2s_tcp_lostlen) as common_c2s_tcp_lostlen,\n" +
                "\tsum(common_s2c_tcp_lostlen) as common_s2c_tcp_lostlen,\n" +
                "\tsum(common_c2s_tcp_unorder_num) as common_c2s_tcp_unorder_num,\n" +
                "\tsum(common_s2c_tcp_unorder_num) as common_s2c_tcp_unorder_num,\n" +
                "\tsum(common_c2s_byte_retrans) as common_c2s_byte_retrans,\n" +
                "\tsum(common_s2c_byte_retrans) as common_s2c_byte_retrans,\n" +
                "\tsum(common_c2s_pkt_retrans) as common_c2s_pkt_retrans,\n" +
                "\tsum(common_s2c_pkt_retrans) as common_s2c_pkt_retrans,\n" +
                "\tavg(common_establish_latency_ms) as common_establish_latency_ms,\n" +
                "\tavg(http_response_latency_ms) as http_response_latency_ms,\n" +
                "\tavg(ssl_con_latency_ms) as ssl_con_latency_ms,\n" +
                "\tROUND(AVG((cast ((common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))/(cast ((common_c2s_byte_num+common_s2c_byte_num+common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))),4) as packet_loss,\n" +
                "        ROUND(AVG((cast ((common_c2s_pkt_retrans+common_s2c_pkt_retrans) as double))/(cast ((common_c2s_pkt_num+common_s2c_pkt_num) as double))),4) as packet_retrans\n" +
                "FROM\n" +
                "\tbase\n" +
                "group by\n" +
                "\tcommon_server_ip,\n" +
                "\tTUMBLE(stat_time,INTERVAL '1' MINUTE)";
    }

    private String getDomainViewddlSql() {
        return "CREATE VIEW top_base_view_domain as\n" +
                "SELECT\n" +
                "\tdomain as domain,\n" +
                "\tUNIX_TIMESTAMP(CAST(TUMBLE_END(stat_time,INTERVAL '1' MINUTE) as VARCHAR)) as common_recv_time,\n" +
                "\tsum(common_c2s_pkt_num) as common_c2s_pkt_num,\n" +
                "\tsum(common_c2s_byte_num) as common_c2s_byte_num,\n" +
                "\tsum(common_s2c_pkt_num) as common_s2c_pkt_num,\n" +
                "\tsum(common_s2c_byte_num) as common_s2c_byte_num,\n" +
                "\tsum(common_sessions) as common_sessions,\n" +
                "\tsum(common_c2s_tcp_lostlen) as common_c2s_tcp_lostlen,\n" +
                "\tsum(common_s2c_tcp_lostlen) as common_s2c_tcp_lostlen,\n" +
                "\tsum(common_c2s_tcp_unorder_num) as common_c2s_tcp_unorder_num,\n" +
                "\tsum(common_s2c_tcp_unorder_num) as common_s2c_tcp_unorder_num,\n" +
                "\tsum(common_c2s_byte_retrans) as common_c2s_byte_retrans,\n" +
                "\tsum(common_s2c_byte_retrans) as common_s2c_byte_retrans,\n" +
                "\tsum(common_c2s_pkt_retrans) as common_c2s_pkt_retrans,\n" +
                "\tsum(common_s2c_pkt_retrans) as common_s2c_pkt_retrans,\n" +
                "\tavg(common_establish_latency_ms) as common_establish_latency_ms,\n" +
                "\tavg(http_response_latency_ms) as http_response_latency_ms,\n" +
                "\tavg(ssl_con_latency_ms) as ssl_con_latency_ms,\n" +
                "\tROUND(AVG((cast ((common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))/(cast ((common_c2s_byte_num+common_s2c_byte_num+common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))),4) as packet_loss,\n" +
                "        ROUND(AVG((cast ((common_c2s_pkt_retrans+common_s2c_pkt_retrans) as double))/(cast ((common_c2s_pkt_num+common_s2c_pkt_num) as double))),4) as packet_retrans\n" +
                "FROM\n" +
                "\tbase\n" +
                "WHERE\n" +
                "\t(domain <> '') and (domain is not null) and (domain <> '')\n" +
                "group by\n" +
                "\tdomain,\n" +
                "\tTUMBLE(stat_time,INTERVAL '1' MINUTE)";
    }

    private String getAppLabelViewddlSql() {
        return "CREATE VIEW top_base_view_applabel as\n" +
                "SELECT\n" +
                "\tcommon_app_label as common_app_label,\n" +
                "\tUNIX_TIMESTAMP(CAST(TUMBLE_END(stat_time,INTERVAL '1' MINUTE) as VARCHAR)) as common_recv_time,\n" +
                "\tsum(common_c2s_pkt_num) as common_c2s_pkt_num,\n" +
                "\tsum(common_c2s_byte_num) as common_c2s_byte_num,\n" +
                "\tsum(common_s2c_pkt_num) as common_s2c_pkt_num,\n" +
                "\tsum(common_s2c_byte_num) as common_s2c_byte_num,\n" +
                "\tsum(common_sessions) as common_sessions,\n" +
                "\tsum(common_c2s_tcp_lostlen) as common_c2s_tcp_lostlen,\n" +
                "\tsum(common_s2c_tcp_lostlen) as common_s2c_tcp_lostlen,\n" +
                "\tsum(common_c2s_tcp_unorder_num) as common_c2s_tcp_unorder_num,\n" +
                "\tsum(common_s2c_tcp_unorder_num) as common_s2c_tcp_unorder_num,\n" +
                "\tsum(common_c2s_byte_retrans) as common_c2s_byte_retrans,\n" +
                "\tsum(common_s2c_byte_retrans) as common_s2c_byte_retrans,\n" +
                "\tsum(common_c2s_pkt_retrans) as common_c2s_pkt_retrans,\n" +
                "\tsum(common_s2c_pkt_retrans) as common_s2c_pkt_retrans,\n" +
                "\tavg(common_establish_latency_ms) as common_establish_latency_ms,\n" +
                "\tavg(http_response_latency_ms) as http_response_latency_ms,\n" +
                "\tavg(ssl_con_latency_ms) as ssl_con_latency_ms,\n" +
                "\tROUND(AVG((cast ((common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))/(cast ((common_c2s_byte_num+common_s2c_byte_num+common_c2s_tcp_lostlen+common_s2c_tcp_lostlen) as double))),4) as packet_loss,\n" +
                "        ROUND(AVG((cast ((common_c2s_pkt_retrans+common_s2c_pkt_retrans) as double))/(cast ((common_c2s_pkt_num+common_s2c_pkt_num) as double))),4) as packet_retrans\n" +
                "FROM\n" +
                "\tbase\n" +
                "WHERE\n" +
                "\tcommon_app_label <> ''\n" +
                "group by\n" +
                "\tcommon_app_label,\n" +
                "\tTUMBLE(stat_time,INTERVAL '1' MINUTE)";
    }

    private String getInsertClientIpSql() {
        return "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip,'' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_client_ip' as group_by,'sessions' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_sessions DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip, '' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_client_ip' as group_by,'packets' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_s2c_pkt_num+common_c2s_pkt_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip, '' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_client_ip' as group_by,'bytes' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_s2c_byte_num+common_c2s_byte_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip, '' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_client_ip' as group_by,'establish_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_establish_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip,'' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_client_ip' as group_by,'http_response_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY http_response_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip,'' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_client_ip' as group_by,'ssl_con_latency' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY ssl_con_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip,'' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_client_ip' as group_by,'packet_loss' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_loss DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip,'' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_client_ip' as group_by,'packet_retrans' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_retrans DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                "WHERE rownum <= 100)";
    }

    private String getInsertClientIpSql2() {
        return "(SELECT common_client_ip, common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "common_client_ip,'' as common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_client_ip' as group_by,'' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans\n" +
                "FROM\n" +
                "top_base_view_client)\n" +
                ")";
    }

    private String getInsertServerIpSql() {
        return "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label, common_recv_time, 'common_server_ip' as group_by,'sessions' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_sessions DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label, common_recv_time,'common_server_ip' as group_by,'packets' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_c2s_pkt_num+common_s2c_pkt_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_server_ip' as group_by,'bytes' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_c2s_byte_num+common_s2c_byte_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_server_ip' as group_by,'establish_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_establish_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_server_ip' as group_by,'http_response_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY http_response_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_server_ip' as group_by,'ssl_con_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY ssl_con_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label,common_recv_time, 'common_server_ip' as group_by,'packet_loss' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_loss DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label,common_recv_time,'common_server_ip' as group_by,'packet_retrans' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_retrans DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                "WHERE rownum <= 100)";
    }

    private String getInsertServerIpSql2() {
        return "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,common_server_ip,'' as domain,'' as common_app_label, common_recv_time, 'common_server_ip' as group_by,'' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans\n" +
                "FROM\n" +
                "top_base_view_server)\n" +
                ")";
    }

    private String getInsertDomainSql() {
        return "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label, common_recv_time, 'domain' as group_by,'sessions' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_sessions DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label, common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label, common_recv_time,'domain' as group_by,'packets' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_c2s_pkt_num+common_s2c_pkt_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label, common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label, common_recv_time,'domain' as group_by,'bytes' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_c2s_byte_num+common_s2c_byte_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label, common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label, common_recv_time, 'domain' as group_by,'establish_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_establish_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label, common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label,common_recv_time,'domain' as group_by,'http_response_latency' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY http_response_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label, common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label,common_recv_time,'domain' as group_by,'ssl_con_latency' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY ssl_con_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label,common_recv_time,'domain' as group_by,'packet_loss' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_loss DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label,common_recv_time,'domain' as group_by,'packet_retrans' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_retrans DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                "WHERE rownum <= 100)";
    }

    private String getInsertDomainSql2() {
        return "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,domain,'' as common_app_label, common_recv_time, 'domain' as group_by,'' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans\n" +
                "FROM\n" +
                "top_base_view_domain)\n" +
                ")";
    }

    private String getInsertAppLabelSql() {
        return "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label, common_recv_time, 'common_app_label' as group_by,'sessions' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_sessions DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain, common_app_label,common_recv_time, 'common_app_label' as group_by,'packets' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_c2s_pkt_num+common_s2c_pkt_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label, common_recv_time,'common_app_label' as group_by,'bytes' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY (common_c2s_byte_num+common_s2c_byte_num) DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label, common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label, common_recv_time, 'common_app_label' as group_by,'establish_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY common_establish_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label,common_recv_time, 'common_app_label' as group_by,'http_response_latency' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY http_response_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label,common_recv_time,'common_app_label' as group_by,'ssl_con_latency' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY ssl_con_latency_ms DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label,common_recv_time,'common_app_label' as group_by,'packet_loss' as order_by,60 as time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_loss DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)\n" +
                "union all\n" +
                "(SELECT common_client_ip,common_server_ip,domain,common_app_label,common_recv_time,group_by,order_by,time_granularity, common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label,common_recv_time, 'common_app_label' as group_by,'packet_retrans' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans,\n" +
                "ROW_NUMBER() OVER (PARTITION BY common_recv_time ORDER BY packet_retrans DESC) as rownum\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                "WHERE rownum <= 100)";
    }

    private String getInsertAppLabelSql2() {
        return "(SELECT common_client_ip,common_server_ip,domain, common_app_label,common_recv_time, group_by,order_by,time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans FROM\n" +
                "(SELECT\n" +
                "'' as common_client_ip,'' as common_server_ip,'' as domain,common_app_label, common_recv_time, 'common_app_label' as group_by,'' as order_by,60 as time_granularity,common_c2s_pkt_num, common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms,packet_loss,packet_retrans\n" +
                "FROM\n" +
                "top_base_view_applabel)\n" +
                ")";
    }

    private String getCountryCityViewDdlSql() {
        return "CREATE VIEW country_city_view as\n" +
                "SELECT\n" +
                "\tserver_country as server_country,\n" +
                "\tclient_country as client_country,\n" +
                "\tserver_province as server_province,\n" +
                "\tclient_province as client_province,\n" +
                "\tserver_region as server_region,\n" +
                "\tclient_region as client_region,\n" +
                "\tcommon_schema_type as common_schema_type,\n" +
                "\tcommon_l4_protocol as common_l4_protocol,\n" +
                "\tsum(common_c2s_pkt_num) as common_c2s_pkt_num,\n" +
                "\tsum(common_c2s_byte_num) as common_c2s_byte_num,\n" +
                "\tsum(common_s2c_pkt_num) as common_s2c_pkt_num,\n" +
                "\tsum(common_s2c_byte_num) as common_s2c_byte_num,\n" +
                "\tsum(common_sessions) as common_sessions,\n" +
                "\tsum(common_c2s_tcp_lostlen) as common_c2s_tcp_lostlen,\n" +
                "\tsum(common_s2c_tcp_lostlen) as common_s2c_tcp_lostlen,\n" +
                "\tsum(common_c2s_tcp_unorder_num) as common_c2s_tcp_unorder_num,\n" +
                "\tsum(common_s2c_tcp_unorder_num) as common_s2c_tcp_unorder_num,\n" +
                "\tsum(common_c2s_byte_retrans) as common_c2s_byte_retrans,\n" +
                "\tsum(common_s2c_byte_retrans) as common_s2c_byte_retrans,\n" +
                "\tsum(common_c2s_pkt_retrans) as common_c2s_pkt_retrans,\n" +
                "\tsum(common_s2c_pkt_retrans) as common_s2c_pkt_retrans,\n" +
                "\tavg(common_establish_latency_ms) as common_establish_latency_ms,\n" +
                "\tavg(http_response_latency_ms) as http_response_latency_ms,\n" +
                "\tavg(ssl_con_latency_ms) as ssl_con_latency_ms,\n" +
                "\tUNIX_TIMESTAMP(CAST(TUMBLE_END(stat_time,INTERVAL '1' MINUTE) as VARCHAR)) as common_recv_time\n" +
                "FROM\n" +
                "\tbase\n" +
                "WHERE\n" +
                "        client_country <> '' or server_country <> ''\n" +
                "group by\n" +
                "\tserver_country,\n" +
                "\tclient_country,\n" +
                "\tserver_province,\n" +
                "\tclient_province,\n" +
                "\tserver_region,\n" +
                "\tclient_region,\n" +
                "\tcommon_schema_type,\n" +
                "\tcommon_l4_protocol,\n" +
                "\tTUMBLE(stat_time,INTERVAL '1' MINUTE)";
    }

    private String getCountryCitySql() {
        return "(SELECT server_country, client_country, server_province, client_province, server_region, client_region, common_recv_time, common_schema_type, common_l4_protocol, common_c2s_pkt_num,common_c2s_byte_num, common_s2c_pkt_num,common_s2c_byte_num,common_sessions,common_c2s_tcp_lostlen,common_s2c_tcp_lostlen,common_c2s_tcp_unorder_num,common_s2c_tcp_unorder_num,common_c2s_byte_retrans,common_s2c_byte_retrans,common_c2s_pkt_retrans,common_s2c_pkt_retrans,common_establish_latency_ms,http_response_latency_ms,ssl_con_latency_ms \n" +
                "FROM\n" +
                "country_city_view\n" +
                ")";
    }

    private static void clientIpMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        KeyedStream<TopBaseLog, Long> keyedStream = source
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_client_ip()))
                .keyBy(CnRecordLog::getCommon_client_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_client_ip", false), new MyWindowFunction())
//                .process(new PreAggregationProcess("common_client_ip"))
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .keyBy(TopBaseLog::getCommon_recv_time);

        commonMetrics(keyedStream, "common_client_ip");
    }

    private static void clientIpMetricsProcess2(SingleOutputStreamOperator<CnRecordLog> source) {
        source.filter((log) -> StringUtil.isNotEmpty(log.getCommon_client_ip()))
                .keyBy(CnRecordLog::getCommon_client_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_client_ip", false), new MyWindowFunction())
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);
    }

    private static void serverIpMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        KeyedStream<TopBaseLog, Long> keyedStream = source
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_server_ip", false), new MyWindowFunction())
//                .process(new PreAggregationProcess("common_server_ip"))
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .keyBy(TopBaseLog::getCommon_recv_time);

        commonMetrics(keyedStream, "common_server_ip");

    }

    private static void serverIpMetricsProcess2(SingleOutputStreamOperator<CnRecordLog> source) {
        source.filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_server_ip", false), new MyWindowFunction())
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

    }

    private static void domainMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        KeyedStream<TopBaseLog, Long> keyedStream = source
                .filter((log) -> StringUtil.isNotEmpty(log.getDomain()))
                .keyBy(CnRecordLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("domain", false), new MyWindowFunction())
//                .process(new PreAggregationProcess("domain"))
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .keyBy(TopBaseLog::getCommon_recv_time);

        commonMetrics(keyedStream, "domain");

    }

    private static void domainMetricsProcess2(SingleOutputStreamOperator<CnRecordLog> source) {
        source.filter((log) -> StringUtil.isNotEmpty(log.getDomain()))
                .keyBy(CnRecordLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("domain", false), new MyWindowFunction())
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

    }

    private static void appLableMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        KeyedStream<TopBaseLog, Long> keyedStream = source
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnRecordLog::getCommon_app_label)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_app_label", false), new MyWindowFunction())
//                .process(new PreAggregationProcess("common_app_label"))
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .keyBy(TopBaseLog::getCommon_recv_time);

        commonMetrics(keyedStream, "common_app_label");
    }

    private static void appLableMetricsProcess2(SingleOutputStreamOperator<CnRecordLog> source) {
        source.filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnRecordLog::getCommon_app_label)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_app_label", false), new MyWindowFunction())
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);
    }

    private static void countryCityMetrics(SingleOutputStreamOperator<CnRecordLog> source) {
        SingleOutputStreamOperator<String> aggregate = source
                .filter((log) -> StringUtil.isNotEmpty(log.getClient_country()) || StringUtil.isNotEmpty(log.getServer_country()))
                .keyBy(new KeysSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("region", true))
                .setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM)
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString);

        aggregate.addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_COUNTRY_CITY_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_COUNTRY_CITY_PARALLELISM);
    }

    private static void commonMetrics(KeyedStream<TopBaseLog, Long> keyedStream, String groupBy) {
        DataStream<String> union = topMetricsProcess(keyedStream, groupBy, "sessions")
                .union(topMetricsProcess(keyedStream, groupBy, "packets"))
                .union(topMetricsProcess(keyedStream, groupBy, "bytes"))
                .union(topMetricsProcess(keyedStream, groupBy, "establish_latency"))
                .union(topMetricsProcess(keyedStream, groupBy, "http_response_latency"))
                .union(topMetricsProcess(keyedStream, groupBy, "ssl_con_latency"))
                .union(topMetricsProcess(keyedStream, groupBy, "packet_loss"))
                .union(topMetricsProcess(keyedStream, groupBy, "packet_retrans"));

        union.addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

    }

    private static SingleOutputStreamOperator<String> topMetricsProcess(KeyedStream<TopBaseLog, Long> keyedStream,
                                                                        String groupBy,
                                                                        String orderBy) {
        SingleOutputStreamOperator<String> map = keyedStream.process(new TopMetricProcessV2(groupBy, orderBy)).filter(Objects::nonNull).map(JsonMapper::toJsonString);
        switch (com.zdjizhi.top.common.CommonConfig.TOP_DEFAULT_SLOTSHARINGGROUP_TYPE) {
            case 1:
                return map.slotSharingGroup(groupBy).setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM);
            case 0:
                return map.slotSharingGroup("default").setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class KeysSelector implements KeySelector<CnRecordLog, Tuple8<String, String, String, String, String, String, String, String>> {
        @Override
        public Tuple8<String, String, String, String, String, String, String, String> getKey(CnRecordLog log) throws Exception {

            return Tuple8.of(log.getServer_country(), log.getClient_country(), log.getServer_province(),
                    log.getClient_province(), log.getServer_region(), log.getClient_region(), log.getCommon_schema_type(), log.getCommon_l4_protocol());
        }
    }

    private static class MyWindowFunction implements WindowFunction<TopBaseLog, TopBaseLog, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<TopBaseLog> input, Collector<TopBaseLog> out) throws Exception {
            TopBaseLog baseLog = input.iterator().next();
            baseLog.setCommon_recv_time(window.getEnd() / 1000);
            out.collect(baseLog);
        }
    }

    public static void main(String[] args) throws Exception {
        new CnTopMetrics().schedule();
    }


}
