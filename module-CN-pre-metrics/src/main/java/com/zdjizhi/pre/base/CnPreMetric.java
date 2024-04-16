package com.zdjizhi.pre.base;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.sink.clickhouse.ClickHouseTableFactory;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.CommonConfig;
import com.zdjizhi.pre.base.common.MetricKeyConfig;
import com.zdjizhi.pre.base.function.*;
import com.zdjizhi.pre.base.operator.FirstAggregation;
import com.zdjizhi.pre.base.operator.SecondAggregationReduce;
import com.zdjizhi.pre.base.operator.TagSecondAggregationReduce;
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
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("client"))
                .name("clientIpMetricWindow");
        clientIpMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_IP_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_IP_TOPIC)))
                .setParallelism(outputParallelism)
                .name("clientIpMetricSink");

        SingleOutputStreamOperator<String> serverIpMetric = process.filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_IP)
                .keyBy(CnMetricLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("server"))
                .name("serverIpMetricWindow");
        serverIpMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_IP_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_IP_TOPIC)))
                .setParallelism(outputParallelism)
                .name("serverIpMetricSink");

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
                .reduce(new SecondAggregationReduce(), new MetricRegionProcessWindowFunc("client"))
                .name("clientRegionMetricWindow");
        clientRegionMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_REGION_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_REGION_TOPIC)))
                .setParallelism(outputParallelism)
                .name("clientRegionMetricSink");

        SingleOutputStreamOperator<String> serverRegionMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_LOCATION)
                .keyBy(new KeySelector<CnMetricLog, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple3.of(value.getServer_country_region(), value.getServer_super_admin_area(), value.getServer_admin_area());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricRegionProcessWindowFunc("server"))
                .name("serverRegionMetricWindow");
        serverRegionMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_REGION_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_REGION_TOPIC)))
                .setParallelism(outputParallelism)
                .name("serverRegionMetricSink");

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
                .reduce(new SecondAggregationReduce(), new MetricAsnProcessFunc("client"))
                .name("clientAsnMetricWindow");
        clientAsnMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_ASN_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_ASN_TOPIC)))
                .setParallelism(outputParallelism)
                .name("clientAsnMetricSink");

        SingleOutputStreamOperator<String> serverAsnMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_ASN_ISP)
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getServer_asn(), value.getServer_isp());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricAsnProcessFunc("server"))
                .name("serverAsnMetricWindow");
        serverAsnMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_ASN_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_ASN_TOPIC)))
                .setParallelism(outputParallelism)
                .name("serverAsnMetricSink");

        //metric_application
        SingleOutputStreamOperator<String> appMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.APP)
                .keyBy(CnMetricLog::getCommon_app_label)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricAppProcessFunc())
                .name("appMetricWindow");
        appMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_APPLICATION_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_APPLICATION_TOPIC)))
                .setParallelism(outputParallelism)
                .name("appMetricSink");

        //metric_domain
        SingleOutputStreamOperator<String> domainMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.DOMAIN)
                .keyBy(CnMetricLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricDomainProcessFunc())
                .name("domainMetricWindow");
        domainMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_DOMAIN_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_DOMAIN_TOPIC)))
                .setParallelism(outputParallelism)
                .name("domainMetricSink");

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
                .reduce(new SecondAggregationReduce(), new MetricProtocolProcessFunc())
                .name("protocolMetricWindow");
        protocolMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_PROTOCOL_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_PROTOCOL_TOPIC)))
                .setParallelism(outputParallelism)
                .name("protocolMetricSink");

        //metric_link
        SingleOutputStreamOperator<String> linkMetric = process
                .filter((log) -> log.getMetricKey() == MetricKeyConfig.LINK)
                .keyBy(new KeySelector<CnMetricLog, Tuple12<String, String, String, String, String, String, String, String, Long, Long, String, String>>() {
                    @Override
                    public Tuple12<String, String, String, String, String, String, String, String, Long, Long, String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple12.of(value.getClient_country_region(), value.getClient_super_admin_area(), value.getClient_admin_area(), value.getClient_zone(),
                                value.getServer_country_region(), value.getServer_super_admin_area(), value.getServer_admin_area(), value.getServer_zone(),
                                value.getCommon_in_link_id(), value.getCommon_out_link_id(),
                                value.getIn_link_direction(), value.getOut_link_direction());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricLinkProcessFunc())
                .name("linkMetricWindow");
        linkMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_LINK_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_LINK_TOPIC)))
                .setParallelism(outputParallelism)
                .name("linkMetricSink");

        SingleOutputStreamOperator<String> subscriberAppMetric = process.filter((log) -> log.getMetricKey() == MetricKeyConfig.SUBSCRIBER_APP)
                .keyBy(new KeySelector<CnMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CnMetricLog value) throws Exception {
                        return Tuple2.of(value.getSubscriber_id(), value.getCommon_app_label());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricSubscriberAppProcessFunc())
                .name("subscriberAppMetricWindow");
        subscriberAppMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_SUBSCRIBER_APP_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_SUBSCRIBER_APP_TOPIC)))
                .setParallelism(outputParallelism)
                .name("subscriberAppMetricSink");


        SingleOutputStreamOperator<String> tagMetric = process.filter((log) -> log.getMetricKey() == MetricKeyConfig.TAG)
                .keyBy(CnMetricLog::getTag)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new TagSecondAggregationReduce(), new MetricTagProcessFunc())
                .name("tagMetricWindow");
        tagMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_TAG_TABLE)))
                .setParallelism(outputParallelism)
                .name("tagMetricSink");
    }

}
