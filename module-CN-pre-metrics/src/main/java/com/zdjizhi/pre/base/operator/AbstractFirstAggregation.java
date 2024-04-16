package com.zdjizhi.pre.base.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.base.common.CommonConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractFirstAggregation<OUT> extends ProcessFunction<CnRecordLog, OUT> {

    private final Logger logger = LoggerFactory.getLogger(AbstractFirstAggregation.class);

    private long windowSize = Time.minutes(1).toMilliseconds();

    private Long staggerOffset = null;

    private long count = 0;

    private Long nextPrintTime = null;

    private final PriorityQueue<Long> eventTimeTimersQueue = new PriorityQueue<>();

    private final Map<Long, Map<String, Map<String, OUT>>> windows = new HashMap<>();

    private Counter inEvents;

    private Counter outEvents;

    protected Counter errorEvents;

    private Counter delayEvents;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        this.windowSize = Time.minutes(configuration.get(CommonConfig.PRE_METRICS_WINDOW_TIME)).toMilliseconds();
        MetricGroup internalMetrics = getRuntimeContext().getMetricGroup().addGroup("internal_metrics");
        inEvents = internalMetrics.counter("in_events");
        outEvents = internalMetrics.counter("out_events");
        errorEvents = internalMetrics.counter("error_events");
        delayEvents = internalMetrics.counter("delay_events");
        internalMetrics.meter("delay_events_rate", new MeterView(delayEvents));
    }

    @Override
    public void processElement(CnRecordLog value, ProcessFunction<CnRecordLog, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        Long timestamp;
        while ((timestamp = eventTimeTimersQueue.peek()) != null && timestamp <= ctx.timerService().currentWatermark()) {
            eventTimeTimersQueue.poll();
            onTimer(timestamp, out);
        }
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        if (nextPrintTime != null && nextPrintTime <= currentProcessingTime) {
            logger.info(currentProcessingTime + ":" + this.getClass().getName() + " one minute late data count :" + count);
            count = 0;
            nextPrintTime = null;
        }
        inEvents.inc();
        if (ctx.timestamp() <= ctx.timerService().currentWatermark()) {
            count++;
            delayEvents.inc();
            if (nextPrintTime == null) {
                nextPrintTime = currentProcessingTime + 60 * 1000;
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
                outEvents.inc();
                out.collect(outLog);
            }
            value.clear();
        }
        accumulator.clear();
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
