package com.zdjizhi.dos.sink;

import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.dos.EtlProcessFunction;
import com.zdjizhi.dos.KeysSelector;
import com.zdjizhi.dos.common.CommonConfig;
import com.zdjizhi.etl.DosSketchEtl;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class DosMetricsSink implements Schedule {

    @Override
    public void schedule() throws Exception {
        DosSketchEtl.getSketchSource().assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(10),
                        (event, timestamp) -> event.getSketch_start_time() * 1000))
                .keyBy(new KeysSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new EtlProcessFunction())
                .print()
                .setParallelism(1);
    }


}
