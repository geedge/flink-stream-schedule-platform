package com.zdjizhi.pre.location;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.sink.clickhouse.ClickHouseTableFactory;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.pre.location.common.CommonConfig;
import com.zdjizhi.pre.location.common.LocationSubscriber;
import com.zdjizhi.pre.location.function.MetricSubscriberProcessWindowFunc;
import com.zdjizhi.pre.location.operator.FirstAggregation;
import com.zdjizhi.pre.location.operator.SecondAggregationReduce;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:35
 */
public class LocationMetric implements Schedule {

    private int windowsTime = 1;

    private int outputParallelism = 1;

    public LocationMetric() {
        this.windowsTime = Configs.get(CommonConfig.LOCATION_METRICS_WINDOW_TIME);
        this.outputParallelism = Configs.get(CommonConfig.LOCATION_METRIC_OUTPUT_PARALLELISM);
    }

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;
        SingleOutputStreamOperator<LocationSubscriber> process = source.process(new FirstAggregation()).name("locationFirstAggProcess");

        SingleOutputStreamOperator<String> locationSubscriberMetric = process.keyBy(LocationSubscriber::getSubscriber_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowsTime)))
                .reduce(new SecondAggregationReduce(), new MetricSubscriberProcessWindowFunc());

        locationSubscriberMetric
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.LOCATION_SUBSCRIBER_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.LOCATION_SUBSCRIBER_TOPIC)))
                .setParallelism(outputParallelism);

        //todo GMLC数据源字段详情不清楚，后续再开发GMLC->location_subscriber的数据流
    }
}
