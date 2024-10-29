package com.zdjizhi.pre.location.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.base.operator.AbstractFirstAggregation;
import com.zdjizhi.pre.location.common.LocationSubscriber;
import com.zdjizhi.pre.location.utils.SubscriberUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:40
 */
public class FirstAggregation extends AbstractFirstAggregation<LocationSubscriber> {

    private final transient Logger logger = LoggerFactory.getLogger(FirstAggregation.class);

    private static final String LOCATION_SUBSCRIBER_KEY = "locationSubscriberMetric";

    @Override
    public Map<String, Map<String, LocationSubscriber>> createAccumulator() {
        Map<String, Map<String, LocationSubscriber>> accumulator = new HashMap<>();
        accumulator.put(LOCATION_SUBSCRIBER_KEY, new HashMap<>());
        return accumulator;
    }

    @Override
    public void add(CnRecordLog value, Map<String, Map<String, LocationSubscriber>> accumulator) {
        try {
            SubscriberUtils.Locate locate = SubscriberUtils.getLocateByURL(value.getHttp_url());
            if (locate == null || locate.getLongitude() == null || locate.getLatitude() == null) {
                return;
            }
            String subscriberId = value.getSubscriber_id();
            if (StringUtils.isNotBlank(subscriberId)) {
                Map<String, LocationSubscriber> locationSubscriberMap = accumulator.get(LOCATION_SUBSCRIBER_KEY);
                LocationSubscriber locationSubscriber = locationSubscriberMap.get(subscriberId);
                if (locationSubscriber != null) {
                    updateLocationSubscriber(value, locationSubscriber);
                } else {
                    locationSubscriber = new LocationSubscriber();
                    locationSubscriber.setSubscriber_id(subscriberId);
                    updateLocationSubscriber(value, locationSubscriber);
                }
                locationSubscriber.setSubscriber_longitude(locate.getLongitude());
                locationSubscriber.setSubscriber_latitude(locate.getLatitude());
                locationSubscriberMap.put(subscriberId, locationSubscriber);
            }
        } catch (Exception e) {
            logger.error("FirstAggregation add error", e);
        }
    }

    private void updateLocationSubscriber(CnRecordLog value, LocationSubscriber locationSubscriber) {
        locationSubscriber.setImei(value.getImei());
        locationSubscriber.setImsi(value.getImsi());
        locationSubscriber.setPhone_number(value.getPhone_number());
        locationSubscriber.setApn(value.getApn());
        locationSubscriber.setRecv_time(value.getCommon_recv_time());
    }
}
