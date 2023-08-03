package com.zdjizhi.pre.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.common.CommonConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractFirstAggregation<OUT> extends ProcessFunction<CnRecordLog, OUT> {

    private final long windowSize = Time.minutes(CommonConfig.PRE_METRICS_WINDOW_TIME).toMilliseconds();

    private Long staggerOffset = null;

    private long count = 0;

    private Long nextPrintTime = null;

    private final PriorityQueue<Long> eventTimeTimersQueue = new PriorityQueue<>();

    private final Map<Long, Map<String, Map<String, OUT>>> windows = new HashMap<>();

    @Override
    public void processElement(CnRecordLog value, ProcessFunction<CnRecordLog, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        Long timestamp;
        while ((timestamp = eventTimeTimersQueue.peek()) != null && timestamp <= ctx.timerService().currentWatermark()) {
            eventTimeTimersQueue.poll();
            onTimer(timestamp, out);
        }
        if (ctx.timestamp() <= ctx.timerService().currentWatermark()) {
            count++;
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            if (nextPrintTime == null || nextPrintTime <= currentProcessingTime) {
                nextPrintTime = currentProcessingTime + 60 * 1000;
                System.out.println("first aggregation count :" + count);
            }
            return;
        }
        long windowEnd = assignWindowEnd(ctx.timestamp());
        if (!windows.containsKey(windowEnd)) {
            windows.put(windowEnd, createAccumulator());
            eventTimeTimersQueue.add(windowEnd);
        }
        add(value, windows.get(windowEnd));
    }

    public void onTimer(long timestamp, Collector<OUT> out) throws Exception {
        Map<String, Map<String, OUT>> accumulator = windows.remove(timestamp);
        for (Map<String, OUT> value : accumulator.values()) {
            for (OUT outLog : value.values()) {
                out.collect(outLog);
            }
        }
    }

    private long assignWindowStart(long timestamp, long offset) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }

    private long assignWindowEnd(long timestamp) {
        if (staggerOffset == null) {
            staggerOffset = getStaggerOffset();
        }
        return assignWindowStart(timestamp, (staggerOffset % windowSize)) + windowSize;
    }

    private long getStaggerOffset() {
        return (long)(ThreadLocalRandom.current().nextDouble() * (double)windowSize);
    }

    public abstract Map<String, Map<String, OUT>> createAccumulator();

    public abstract void add(CnRecordLog value, Map<String, Map<String, OUT>> accumulator);
}
