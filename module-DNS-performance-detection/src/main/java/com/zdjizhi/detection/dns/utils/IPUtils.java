package com.zdjizhi.detection.dns.utils;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.utils.IpLookupV2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class IPUtils {
    public static IpLookupV2 ipLookup;
    private static final String FILE_SUFFIX = ".mmdb";

    static {
        IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
        byte[] asnv4Bytes = FileUtils.getFileBytes(CommonInternalConfig.ASNV4_TYPE + FILE_SUFFIX);
        if (asnv4Bytes !=null){
            InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
            builder.loadAsnDataFileV4(asnv4InputStream);
        }
        byte[] asnv6Bytes = FileUtils.getFileBytes(CommonInternalConfig.ASNV6_TYPE + FILE_SUFFIX);
        if (asnv6Bytes !=null){
            InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
            builder.loadAsnDataFileV6(asnv6InputStream);
        }
        ipLookup = builder.build();
    }
}
