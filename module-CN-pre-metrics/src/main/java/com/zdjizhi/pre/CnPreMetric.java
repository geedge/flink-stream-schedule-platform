package com.zdjizhi.pre;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.CommonConfig;
import com.zdjizhi.pre.common.MetricKeyConfig;
import com.zdjizhi.pre.function.*;
import com.zdjizhi.pre.operator.FirstAggregation;
import com.zdjizhi.pre.operator.SecondAggregationReduce;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CnPreMetric implements Schedule {

    private int windowsTime = 1;

    private int outputParallelism = 1;

    public CnPreMetric() {
        this.windowsTime = Configs.get(CommonConfig.PRE_METRICS_WINDOW_TIME);
        this.outputParallelism = Configs.get(CommonConfig.METRIC_OUTPUT_PARALLELISM);
    }

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;
        SingleOutputStreamOperator<CnMetricLog> process = source.process(new FirstAggregation()).name("preProcess");

        //metric_ip
        SingleOutputStreamOperator<String> clientIpMetric = process.filter((log) -> log.getMetricKey() == MetricKeyConfig.CLIENT_IP)
                .keyBy(CnMetricLog::getCommon_client_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("client"));
        clientIpMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_IP_TOPIC))).setParallelism(outputParallelism);

        SingleOutputStreamOperator<String> serverIpMetric = process.filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_IP)
                .keyBy(CnMetricLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("server"));
        serverIpMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_IP_TOPIC))).setParallelism(outputParallelism);

        //metric_region
        SingleOutputStreamOperator<String> clientRegionMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.CLIENT_LOCATION)
                .keyBy(new KeySelector<CnMetricLog, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple3.of(value.getClient_country_region(), value.getClient_super_admin_area(), value.getClient_admin_area());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricRegionProcessWindowFunc("client"));
        clientRegionMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_REGION_TOPIC))).setParallelism(outputParallelism);

        SingleOutputStreamOperator<String> serverRegionMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_LOCATION)
                .keyBy(new KeySelector<CnMetricLog, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple3.of(value.getServer_country_region(), value.getServer_super_admin_area(), value.getServer_admin_area());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricRegionProcessWindowFunc("server"));
        serverRegionMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_REGION_TOPIC))).setParallelism(outputParallelism);

//        //metric_asn
        SingleOutputStreamOperator<String> clientAsnMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.CLIENT_ASN_ISP)
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getClient_asn(), value.getClient_isp());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricAsnProcessFunc("client"));
        clientAsnMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_ASN_TOPIC))).setParallelism(outputParallelism);

        SingleOutputStreamOperator<String> serverAsnMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_ASN_ISP)
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getServer_asn(), value.getServer_isp());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricAsnProcessFunc("server"));
        serverAsnMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_ASN_TOPIC))).setParallelism(outputParallelism);

        //metric_application
        SingleOutputStreamOperator<String> appMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.APP)
                .keyBy(CnMetricLog::getCommon_app_label)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricAppProcessFunc());
        appMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_APPLICATION_TOPIC))).setParallelism(outputParallelism);

        //metric_domain
        SingleOutputStreamOperator<String> domainMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.DOMAIN)
                .keyBy(CnMetricLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricDomainProcessFunc());
        domainMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_DOMAIN_TOPIC))).setParallelism(outputParallelism);

        //metric_protocol
        SingleOutputStreamOperator<String> protocolMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.PROTOCOL_PORT)
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getCommon_l7_protocol(), value.getCommon_server_port());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricProtocolProcessFunc());
        protocolMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_PROTOCOL_TOPIC))).setParallelism(outputParallelism);

        //metric_link
        SingleOutputStreamOperator<String> linkMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.LINK)
                .keyBy(new KeySelector<CnMetricLog, Tuple12<String, String, String, String, String, String, String, String, Long, Long ,String, String>>() {
                    @Override
                    public Tuple12<String, String, String, String, String, String, String, String, Long, Long ,String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple12.of(value.getClient_country_region(), value.getClient_super_admin_area(), value.getClient_admin_area(), value.getClient_zone(),
                                value.getServer_country_region(), value.getServer_super_admin_area(), value.getServer_admin_area(), value.getServer_zone(),
                                value.getCommon_in_link_id(), value.getCommon_out_link_id(),
                                value.getIn_link_direction(), value.getOut_link_direction());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricLinkProcessFunc());
        linkMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_LINK_TOPIC))).setParallelism(outputParallelism);

        SingleOutputStreamOperator<String> subscriberAppMetric = process.filter((log) -> log.getMetricKey() == MetricKeyConfig.SUBSCRIBER_APP)
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getSubscriber_id(), value.getCommon_app_label());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricSubscriberAppProcessFunc());
        subscriberAppMetric.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_SUBSCRIBER_APP_TOPIC))).setParallelism(outputParallelism);
    }

}
