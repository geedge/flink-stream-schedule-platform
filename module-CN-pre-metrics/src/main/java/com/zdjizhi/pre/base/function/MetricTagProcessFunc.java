package com.zdjizhi.pre.base.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.MetricResultLog;
import org.apache.datasketches.theta.Union;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/7 11:42
 */
public class MetricTagProcessFunc extends ProcessWindowFunction<CnMetricLog, String, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<CnMetricLog, String, String, TimeWindow>.Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setTag(s);
        Union tagServerIpUnion = next.getTag_server_ip_union();
        byte[] ipSketchBytes = tagServerIpUnion.getResult().toByteArray();
        metricResultLog.setIp_sketch(Base64.getEncoder().encodeToString(addHeader(ipSketchBytes)));
        Union tagDomainUnion = next.getTag_domain_union();
        byte[] domainSketchBytes = tagDomainUnion.getResult().toByteArray();
        metricResultLog.setDomain_sketch(Base64.getEncoder().encodeToString(addHeader(domainSketchBytes)));
        metricResultLog.setStat_time(context.window().getStart() / 1000);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }

    public static byte[] getHeaderByteArray(long l) {
        List<Byte> byteList = new ArrayList<>();

        while (l > 0x7F) {
            byte b = (byte) ((l & 0x7F) | 0x80);
            byteList.add(b);
            l >>>= 7;
        }
        byteList.add((byte) l);
        byte[] bytes = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            bytes[i] = byteList.get(i);
        }
        return bytes;
    }

    public static byte[] mergeByteArray(byte[]... bytes) {
        int length = 0;
        for (byte[] aByte : bytes) {
            length += aByte.length;
        }
        byte[] result = new byte[length];
        int index = 0;
        for (byte[] aByte : bytes) {
            System.arraycopy(aByte, 0, result, index, aByte.length);
            index += aByte.length;
        }
        return result;
    }

    public static byte[] addHeader(byte[] bytes) {
        byte[] header = getHeaderByteArray(bytes.length);
        return mergeByteArray(header, bytes);
    }
}
