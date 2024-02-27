package com.zdjizhi.pre.location.function;

import com.alibaba.fastjson2.JSON;
import com.uber.h3core.H3Core;
import com.zdjizhi.pre.location.common.CommonConfig;
import com.zdjizhi.pre.location.common.LocationMetricResult;
import com.zdjizhi.pre.location.common.LocationSubscriber;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 17:02
 */
public class MetricSubscriberProcessWindowFunc extends ProcessWindowFunction<LocationSubscriber, String, String, TimeWindow> {

    private H3Core h3;

    protected int firstRes = 9;

    protected int secondRes = 8;

    protected int thirdRes = 7;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        h3 = H3Core.newInstance();
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        firstRes = configuration.get(CommonConfig.H3_FIRST_RES);
        secondRes = configuration.get(CommonConfig.H3_SECOND_RES);
        thirdRes = configuration.get(CommonConfig.H3_THIRD_RES);
    }

    @Override
    public void process(String s, ProcessWindowFunction<LocationSubscriber, String, String, TimeWindow>.Context context, Iterable<LocationSubscriber> elements, Collector<String> out) throws Exception {
        LocationSubscriber next = elements.iterator().next();
        LocationMetricResult locationMetricResult = new LocationMetricResult();
        locationMetricResult.setSubscriber_id(next.getSubscriber_id());
        locationMetricResult.setImei(next.getImei());
        locationMetricResult.setImsi(next.getImsi());
        locationMetricResult.setPhone_number(next.getPhone_number());
        locationMetricResult.setApn(next.getApn());
        locationMetricResult.setSubscriber_longitude(next.getSubscriber_longitude());
        locationMetricResult.setSubscriber_latitude(next.getSubscriber_latitude());
        locationMetricResult.setFirst_location(h3.latLngToCellAddress(locationMetricResult.getSubscriber_latitude(), locationMetricResult.getSubscriber_longitude(), firstRes));
        locationMetricResult.setSecond_location(h3.latLngToCellAddress(locationMetricResult.getSubscriber_latitude(), locationMetricResult.getSubscriber_longitude(), secondRes));
        locationMetricResult.setThird_location(h3.latLngToCellAddress(locationMetricResult.getSubscriber_latitude(), locationMetricResult.getSubscriber_longitude(), thirdRes));
        locationMetricResult.setStat_time(context.window().getStart() / 1000);
        String metricResultJsonStr = JSON.toJSONString(locationMetricResult);
        out.collect(metricResultJsonStr);
    }
}
