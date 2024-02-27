package com.zdjizhi.pre.location.operator;

import com.zdjizhi.pre.location.common.LocationSubscriber;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:54
 */
public class SecondAggregationReduce implements ReduceFunction<LocationSubscriber> {

    @Override
    public LocationSubscriber reduce(LocationSubscriber value1, LocationSubscriber value2) throws Exception {
        if (value1.getRecv_time() > value2.getRecv_time()) {
            return value1;
        }
        return value2;
    }
}
