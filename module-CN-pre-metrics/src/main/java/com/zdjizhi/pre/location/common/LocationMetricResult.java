package com.zdjizhi.pre.location.common;

import lombok.Data;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:56
 */
@Data
public class LocationMetricResult {
    private String subscriber_id;
    private String imei;
    private String imsi;
    private String phone_number;
    private String apn;
    private Double subscriber_longitude;
    private Double subscriber_latitude;
    private String first_location;
    private String second_location;
    private String third_location;
    private Long stat_time;
}
