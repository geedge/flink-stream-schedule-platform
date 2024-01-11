package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;


public class MetricSubscriberAppProcessFunc extends AbstractMetricProcessFunc<Tuple2<String, String>> {

    private Locate[] locates = new Locate[]{Locate.L1, Locate.L2, Locate.L3, Locate.L4, Locate.L5};

    @Override
    public void setCommonFields(Tuple2<String, String> key, CnMetricLog cnMetricLog, MetricResultLog metricResultLog) {
        metricResultLog.setSubscriber_id(key.f0);
        metricResultLog.setCommon_app_label(key.f1);
        metricResultLog.setImei(cnMetricLog.getImei());
        metricResultLog.setImsi(cnMetricLog.getImsi());
        metricResultLog.setPhone_number(cnMetricLog.getPhone_number());
        metricResultLog.setApn(cnMetricLog.getApn());
        Locate randomLocate = getRandomLocate();
        metricResultLog.setSubscriber_longitude(randomLocate.longitude);
        metricResultLog.setSubscriber_latitude(randomLocate.latitude);
    }

    private Locate getRandomLocate() {
        Random random = new Random();
        int i = random.nextInt(5);
        return locates[i];
    }

    private enum Locate {
        L1(116.391721, 39.906094),
        L2(116.386739, 39.906266),
        L3(116.384863, 39.902834),
        L4(116.387807, 39.900607),
        L5(116.39328, 39.898911);


        private Double longitude;
        private Double latitude;

        Locate(Double longitude, Double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }
}
