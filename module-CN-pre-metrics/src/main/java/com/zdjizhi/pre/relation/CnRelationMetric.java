package com.zdjizhi.pre.relation;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.sink.clickhouse.ClickHouseTableFactory;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.pre.base.common.CommonConfig;
import com.zdjizhi.pre.relation.common.RelationMetricLog;
import com.zdjizhi.pre.relation.function.DomainIpAppFirstAggregation;
import com.zdjizhi.pre.relation.function.SubscriberAppFirstAggregation;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CnRelationMetric implements Schedule {
    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;
        SingleOutputStreamOperator<RelationMetricLog> domainIpAppRelationProcess = source.filter(log -> "TCP".equalsIgnoreCase(log.getCommon_l4_protocol()) || log.getCommon_server_port() == 53 || log.getCommon_server_port() == 443)
                .process(new DomainIpAppFirstAggregation()).name("domainIpAppRelationProcess");

        SingleOutputStreamOperator<String> relationMetric = domainIpAppRelationProcess.filter(log -> log.getMetricKey() == 1)
                .keyBy(new KeySelector<RelationMetricLog, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(RelationMetricLog value) throws Exception {
                        return Tuple3.of(value.getApp_name(), value.getDomain(), value.getIp());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(Configs.get(CommonConfig.PRE_METRICS_WINDOW_TIME))))
                .reduce(new ReduceFunction<RelationMetricLog>() {
                    @Override
                    public RelationMetricLog reduce(RelationMetricLog value1, RelationMetricLog value2) throws Exception {
                        return value1;
                    }
                }, new ProcessWindowFunction<RelationMetricLog, String, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple3<String, String, String> key, ProcessWindowFunction<RelationMetricLog, String, Tuple3<String, String, String>, TimeWindow>.Context context, Iterable<RelationMetricLog> elements, Collector<String> out) throws Exception {
                        RelationMetricLog next = elements.iterator().next();
                        next.setStat_time(context.window().getStart() / 1000);
                        String metricResultJsonStr = JSON.toJSONString(next);
                        out.collect(metricResultJsonStr);
                    }
                }).name("domainIpAppRelationWindow");
        relationMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_RELATION_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_RELATION_TOPIC)))
                .setParallelism(Configs.get(CommonConfig.ENTITY_RELATION_OUTPUT_PARALLELISM))
                .name("domainIpAppRelationSink");


        SingleOutputStreamOperator<String> attributeMetric = domainIpAppRelationProcess.filter(log -> log.getMetricKey() == 2)
                .keyBy(new KeySelector<RelationMetricLog, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> getKey(RelationMetricLog value) throws Exception {
                        return Tuple3.of(value.getIp(), value.getL7_protocol(), value.getPort());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(Configs.get(CommonConfig.PRE_METRICS_WINDOW_TIME))))
                .reduce(new ReduceFunction<RelationMetricLog>() {
                    @Override
                    public RelationMetricLog reduce(RelationMetricLog value1, RelationMetricLog value2) throws Exception {
                        return value1;
                    }
                }, new ProcessWindowFunction<RelationMetricLog, String, Tuple3<String, String, Integer>, TimeWindow>() {
                    @Override
                    public void process(Tuple3<String, String, Integer> key, ProcessWindowFunction<RelationMetricLog, String, Tuple3<String, String, Integer>, TimeWindow>.Context context, Iterable<RelationMetricLog> elements, Collector<String> out) throws Exception {
                        RelationMetricLog next = elements.iterator().next();
                        next.setStat_time(context.window().getStart() / 1000);
                        String metricResultJsonStr = JSON.toJSONString(next);
                        out.collect(metricResultJsonStr);
                    }
                }).name("ipAttributeWindow");
        attributeMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_ATTRIBUTE_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_ATTRIBUTE_TOPIC)))
                .setParallelism(Configs.get(CommonConfig.DYNAMIC_ATTRIBUTE_OUTPUT_PARALLELISM))
                .name("ipAttributeSink");


        SingleOutputStreamOperator<RelationMetricLog> subscriberAppRelationProcess = source.process(new SubscriberAppFirstAggregation()).name("subscriberAppRelationProcess");

        SingleOutputStreamOperator<String> subscriberAppRelationMetric = subscriberAppRelationProcess.filter(log -> log.getMetricKey() == 3)
                .keyBy(new KeySelector<RelationMetricLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(RelationMetricLog value) throws Exception {
                        return Tuple2.of(value.getSubscriber_id(), value.getApp_name());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(Configs.get(CommonConfig.PRE_METRICS_WINDOW_TIME))))
                .reduce(new ReduceFunction<RelationMetricLog>() {
                    @Override
                    public RelationMetricLog reduce(RelationMetricLog value1, RelationMetricLog value2) throws Exception {
                        return value1;
                    }
                }, new ProcessWindowFunction<RelationMetricLog, String, Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, String> key, ProcessWindowFunction<RelationMetricLog, String, Tuple2<String, String>, TimeWindow>.Context context, Iterable<RelationMetricLog> elements, Collector<String> out) throws Exception {
                        RelationMetricLog next = elements.iterator().next();
                        next.setStat_time(context.window().getStart() / 1000);
                        String metricResultJsonStr = JSON.toJSONString(next);
                        out.collect(metricResultJsonStr);
                    }
                })
                .name("subscriberAppRelationWindow");

        subscriberAppRelationMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.METRIC_SUBSCRIBER_APP_RELATION_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.METRIC_SUBSCRIBER_APP_RELATION_TOPIC)))
                .setParallelism(Configs.get(CommonConfig.SUBSCRIBER_APP_RELATION_OUTPUT_PARALLELISM))
                .name("subscriberAppRelationSink");
    }

    //todo GMLC数据源字段详情不清楚，后续再开发GMLC->relation_subscriber_app的数据流
}
