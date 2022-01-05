package com.zdjizhi.dos.sink;

import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.etl.DosSketchEtl;

import java.time.Duration;

/**
 * @author 94976
 */
public class OutputStreamSink implements Schedule {

    @Override
    public void schedule() throws Exception {
        DosSketchEtl.getSketchSource().assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(10),
                        (event, timestamp) -> event.getSketch_start_time() * 1000))
                .print();
    }
}
