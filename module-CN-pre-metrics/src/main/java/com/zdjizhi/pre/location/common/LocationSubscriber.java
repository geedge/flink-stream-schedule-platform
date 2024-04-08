package com.zdjizhi.pre.location.common;

import lombok.Data;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:25
 */
@Data
public class LocationSubscriber {
    private String subscriber_id;
    private String imei;
    private String imsi;
    private String phone_number;
    private String apn;
    private Double subscriber_longitude;
    private Double subscriber_latitude;
    private Long recv_time;
}
