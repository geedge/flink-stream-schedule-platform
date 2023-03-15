package com.zdjizhi.detection.dns.utils;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.utils.IpLookupV2;

public class IPUtils {
    public static IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
            .loadAsnDataFileV4(CommonConfig.IP_MMDB_PATH + "asn_v4.mmdb")
            .loadAsnDataFileV6(CommonConfig.IP_MMDB_PATH + "asn_v6.mmdb")
            .build();

}
