package com.zdjizhi.pre;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.CommonConfig;
import com.zdjizhi.pre.function.*;
import com.zdjizhi.pre.operator.*;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Random;

public class CnPreMetric implements Schedule {

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;
        SingleOutputStreamOperator<CnMetricLog> process = source.map(new AddRandomKey()).keyBy(CnRecordLog::getRandom_num)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .aggregate(new FirstAggregationFunc(), new FirstAggregationProcessFunc<Integer>());

        //metric_ip
        SingleOutputStreamOperator<String> clientIpMetric = process.filter((log) -> StringUtil.isNotEmpty(log.getCommon_client_ip()))
                .keyBy(CnMetricLog::getCommon_client_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("client"));
        clientIpMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_IP_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        SingleOutputStreamOperator<String> serverIpMetric = process.filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnMetricLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("server"));
        serverIpMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_IP_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_region
        SingleOutputStreamOperator<String> clientRegionMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getClient_country()) || StringUtil.isNotEmpty(log.getClient_province()) || StringUtil.isNotEmpty(log.getClient_region()))
                .keyBy(new KeySelector<CnMetricLog, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple3.of(value.getClient_country(), value.getClient_province(), value.getClient_region());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricRegionProcessWindowFunc("client"));
        clientRegionMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_REGION_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        SingleOutputStreamOperator<String> serverRegionMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getServer_country()) || StringUtil.isNotEmpty(log.getServer_province()) || StringUtil.isNotEmpty(log.getServer_region()))
                .keyBy(new KeySelector<CnMetricLog, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple3.of(value.getServer_country(), value.getServer_province(), value.getServer_region());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricRegionProcessWindowFunc("server"));
        serverRegionMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_REGION_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

//        //metric_asn
        SingleOutputStreamOperator<String> clientAsnMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getClient_asn()) && StringUtil.isNotEmpty(log.getClient_isp()))
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getClient_asn(), value.getClient_isp());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricAsnProcessFunc("client"));
        clientAsnMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_ASN_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        SingleOutputStreamOperator<String> serverAsnMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getServer_asn()) && StringUtil.isNotEmpty(log.getServer_isp()))
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getServer_asn(), value.getServer_isp());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricAsnProcessFunc("server"));
        serverAsnMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_ASN_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_idc_renter
        SingleOutputStreamOperator<String> clientIdcMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getClient_idc_renter()))
                .keyBy(CnMetricLog::getClient_idc_renter)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricIdcProcessFunc("client"));
        clientIdcMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_IDC_RENTER_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        SingleOutputStreamOperator<String> serverIdcMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getServer_idc_renter()))
                .keyBy(CnMetricLog::getServer_idc_renter)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricIdcProcessFunc("server"));
        serverIdcMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_IDC_RENTER_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_application
        SingleOutputStreamOperator<String> appMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnMetricLog::getCommon_app_label)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricAppProcessFunc());
        appMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_APPLICATION_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_domain
        SingleOutputStreamOperator<String> domainMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getDomain()))
                .keyBy(CnMetricLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDomainProcessFunc());
        domainMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DOMAIN_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_http_host
        SingleOutputStreamOperator<String> httpHostMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getHttp_host()))
                .keyBy(CnMetricLog::getHttp_host)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricHostProcessFunc());
        httpHostMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_HTTP_HOST_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_ssl_sni
        SingleOutputStreamOperator<String> sslSniMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getSsl_sni()))
                .keyBy(CnMetricLog::getSsl_sni)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricSslProcessFunc());
        sslSniMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_SSL_SNI_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_protocol
        SingleOutputStreamOperator<String> protocolMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_l7_protocol()) && StringUtil.isNotEmpty(log.getCommon_server_port()))
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getCommon_l7_protocol(), value.getCommon_server_port());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricProtocolProcessFunc());
        protocolMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_PROTOCOL_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

        //metric_link
        SingleOutputStreamOperator<String> linkMetric = process
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_ingress_link_id()) || StringUtil.isNotEmpty(log.getCommon_egress_link_id()))
                .keyBy(new KeySelector<CnMetricLog, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getCommon_ingress_link_id(), value.getCommon_egress_link_id());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricLinkProcessFunc());
        linkMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_LINK_TOPIC)).setParallelism(CommonConfig.METRIC_OUTPUT_PARALLELISM);

    }

}
