package com.zdjizhi.etl.utils.ip;

import com.geedgenetworks.model.LocationResponse;
import com.geedgenetworks.utils.IpLookupV2;
import com.geedgenetworks.utils.StringUtil;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;

public class IPUtils {
    public static IpLookupV2 ipLookup;

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static Exception exceptionCache;

    private static int exceptionCount = 0;

    private static boolean needReload = true;

    private static final IpLookupV2.Builder builder = new IpLookupV2.Builder(false);

    public static void init(String type, InputStream inputStream, Configuration configuration) {
        needReload = true;
        if (configuration.get(CommonInternalConfig.IP_TYPE).equals(type)) {
            builder.loadDataFile(inputStream);
        }
        if (configuration.get(CommonInternalConfig.ASN_TYPE).equals(type)) {
            builder.loadAsnDataFile(inputStream);
        }
    }

    public static void load() {
        if (needReload) {
            ipLookup = builder.build();
            needReload = false;
        }
    }

    public static void setMmdbMessage(CnRecordLog entity) {
        try {
            String clientIp = entity.getCommon_client_ip();
            Object clientObject = ipLookup.infoLookup(clientIp);
            if (clientObject instanceof LocationResponse) {
                LocationResponse response = (LocationResponse) clientObject;
                entity.setClient_country_region(response.getCountry());
                entity.setClient_super_admin_area(response.getSuperAdministrativeArea());
                entity.setClient_admin_area(response.getAdministrativeArea());
                entity.setClient_isp(response.getIsp());
                entity.setClient_longitude(formatStringtoDouble(response.getLongitude()));
                entity.setClient_latitude(formatStringtoDouble(response.getLatitude()));
            }
            entity.setClient_asn(ipLookup.asnLookup(clientIp));

            String serverIp = entity.getCommon_server_ip();
            Object serverObject = ipLookup.infoLookup(serverIp);
            if (serverObject instanceof LocationResponse) {
                LocationResponse response = (LocationResponse) serverObject;
                entity.setServer_country_region(response.getCountry());
                entity.setServer_super_admin_area(response.getSuperAdministrativeArea());
                entity.setServer_admin_area(response.getAdministrativeArea());
                entity.setServer_isp(response.getIsp());
                entity.setServer_longitude(formatStringtoDouble(response.getLongitude()));
                entity.setServer_latitude(formatStringtoDouble(response.getLatitude()));
            }
            entity.setServer_asn(ipLookup.asnLookup(serverIp));
        } catch (Exception e) {
            if (exceptionEquals(exceptionCache, e)) {
                exceptionCount++;
                if (exceptionCount < 3) {
                    LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
                }
                if (exceptionCount == 3) {
                    LOG.error("相同的错误异常次数超过3次,后续相同的异常会被隐藏");
                }
            } else {
                exceptionCache = e;
                exceptionCount = 0;
                LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
            }
        }
    }

    private static boolean exceptionEquals(Exception e1, Exception e2) {
        if (e1 == null) {
            return false;
        }
        if (!e1.toString().equals(e2.toString())) {
            return false;
        }
        return Arrays.equals(e1.getStackTrace(), e2.getStackTrace());
    }

    private static Double formatStringtoDouble(String value) {
        Double result = 0.0;
        if ("N/A".equals(value) || StringUtil.isBlank(value)) {
            return result;
        } else {
            return Double.valueOf(value);
        }
    }

    public static void close() {
        ipLookup = null;
    }
}