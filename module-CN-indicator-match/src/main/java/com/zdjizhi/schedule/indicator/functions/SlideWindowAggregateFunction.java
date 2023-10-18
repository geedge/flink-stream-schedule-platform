package com.zdjizhi.schedule.indicator.functions;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.schedule.indicator.record.SlideAggregate;
import org.apache.flink.api.common.functions.AggregateFunction;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Aggregating CnRecordLog elements within a sliding window.
 *
 * @author chaoc
 * @since 23.10
 */
public class SlideWindowAggregateFunction
        implements AggregateFunction<CnRecordLog, SlideAggregate<CnRecordLog>, SlideAggregate<CnRecordLog>> {

    @Override
    public SlideAggregate<CnRecordLog> createAccumulator() {
        return new SlideAggregate<>();
    }

    @Override
    public SlideAggregate<CnRecordLog> add(CnRecordLog log, SlideAggregate<CnRecordLog> accumulator) {
        accumulator.setValue(log);
        accumulator.incTimes();
        accumulator.updateTime(log.getCommon_recv_time() * 1000);
        return accumulator;
    }

    @Override
    public SlideAggregate<CnRecordLog> getResult(SlideAggregate<CnRecordLog> accumulator) {
        return accumulator;
    }

    @Override
    public SlideAggregate<CnRecordLog> merge(SlideAggregate<CnRecordLog> a, SlideAggregate<CnRecordLog> b) {
        return new SlideAggregate<>(a.getValue(), a.getTimes() + b.getTimes(),
                min(a.getFirstTime(), b.getFirstTime()),
                max(a.getLastTime(), b.getLastTime()));
    }
}
