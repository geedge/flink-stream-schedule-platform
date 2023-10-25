package com.zdjizhi.schedule.indicator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.schedule.indicator.common.Configs;
import com.zdjizhi.schedule.indicator.common.serialization.FastjsonObjectSerializationSchema;
import com.zdjizhi.schedule.indicator.functions.*;
import com.zdjizhi.schedule.indicator.record.SecurityEvent;
import com.zdjizhi.schedule.indicator.record.SlideAggregate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// Indicator Match
public class IndicatorSchedule implements Schedule {

    @Override
    public void schedule() throws Exception {

        final DataStream<CnRecordLog> sourceStream = CnRecordEtl.singleOutputStreamOperator;

        final DataStream<CnRecordLog> filteredStream =
                sourceStream.filter(new RuleIDFilterFunction())
                        .name("RuleIDFilter").uid("rule-id-filter");

        final DataStream<CnRecordLog> flattenedStream =
                filteredStream.flatMap(new RuleIDFlatMapFunction())
                        .name("RuleIDFlatten").uid("rule-id-flattened");

        final MatchKeySelector keySelector = new MatchKeySelector();

        final DataStream<SlideAggregate<CnRecordLog>> slideStream =
                flattenedStream.keyBy(keySelector)
                        .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                        .aggregate(new SlideWindowAggregateFunction())
                        .name("SlideAggregate").uid("slide-aggregate");

        final DataStream<SecurityEvent> securityStream =
                slideStream.keyBy(new SlidedKeySelector(keySelector))
                        .process(new MatchKeyedProcessFunction())
                        .name("MatcherAndTrigger").uid("match-and-trigger");

        final Configuration configuration = Configs.create();

        securityStream.map(new FastjsonObjectSerializationSchema<>())
                .name("FastjsonObjectSerialization").uid("fastjson-object-serialization")
                .addSink(KafkaUtils.getKafkaSink(configuration.get(Configs.SINK_TOPIC_NAME)))
                .name("IndicatorMatchSink").uid("indicator-match-sink");
    }
}
