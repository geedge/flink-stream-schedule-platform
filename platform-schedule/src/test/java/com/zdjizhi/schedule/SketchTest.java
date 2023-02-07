package com.zdjizhi.schedule;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.utils.JsonMapper;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.*;

public class SketchTest {

    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<String> process = CnRecordEtl.singleOutputStreamOperator
                .keyBy(CnRecordLog::getCommon_schema_type)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .process(new ProcessWindowFunction<CnRecordLog, String, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<CnRecordLog> elements, Collector<String> out) throws Exception {
                        Result result = new Result();
                        result.setSchema_type(s);
                        result.setCommon_recv_time(System.currentTimeMillis() / 1000);
                        HllSketch sketch = new HllSketch(12);

                        ItemsSketch<String> itemsSketch = new ItemsSketch<String>(64);

                        UpdateSketch thetaSketch = UpdateSketch.builder().build();

                        HashSet<String> strings = new HashSet<>();
                        for (CnRecordLog cnRecordLog : elements) {
                            result.setC2s_pkt_num(cnRecordLog.getCommon_c2s_pkt_num() + result.getC2s_pkt_num());
                            result.setS2c_pkt_num(cnRecordLog.getCommon_s2c_pkt_num() + result.getS2c_pkt_num());
                            result.setC2s_byte_num(cnRecordLog.getCommon_c2s_byte_num() + result.getC2s_byte_num());
                            result.setS2c_byte_num(cnRecordLog.getCommon_s2c_byte_num() + result.getS2c_byte_num());

                            String commonClientIp = cnRecordLog.getCommon_client_ip();
                            sketch.update(commonClientIp);

                            thetaSketch.update(commonClientIp);

                            itemsSketch.update(commonClientIp);

                            strings.add(commonClientIp);

                        }

//                        result.setIp_object(BASE64_ENCODER.encodeToString(sketch.toCompactByteArray()));
                        byte[] ipObject = sketch.toCompactByteArray();
                        result.setIp_object(ipObject);

                        result.setItemsSketch(itemsSketch.toByteArray(new ArrayOfStringsSerDe()));

                        result.setThetaSketch(thetaSketch.toByteArray());

//                        ItemsSketch<String> itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(itemsSketch.toByteArray(new ArrayOfStringsSerDe())), new ArrayOfStringsSerDe());
//                        union.merge(itemsSketch);


                        System.out.println("-------------");
                        ItemsSketch.Row<String>[] items = itemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
                        System.out.println("Frequent items: " + items.length);
                        System.out.println(ItemsSketch.Row.getRowHeader());
                        for (ItemsSketch.Row<String> row: items) {
                            System.out.println(row.toString());
                        }
                        System.out.println(Arrays.toString(strings.toArray()));
                        System.out.println(sketch.getEstimate()+"--"+strings.size());

                        System.out.println("Union unique count estimate: " + thetaSketch.getEstimate());
                        System.out.println("Union unique count lower bound 95% confidence: " + thetaSketch.getLowerBound(2));
                        System.out.println("Union unique count upper bound 95% confidence: " + thetaSketch.getUpperBound(2));

                        System.out.println("--------------");
                        out.collect(JsonMapper.toJsonString(result));

                    }
                });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.44.12:9094");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + CommonConfig.SASL_JAAS_CONFIG_USER + "\" password=\"" + CommonConfig.SASL_JAAS_CONFIG_PASSWORD + "\";");


        process.addSink(new FlinkKafkaProducer<String>(
                "test",
                new SimpleStringSchema(),
                properties,
                Optional.empty()
        ));

        FlinkEnvironmentUtils.streamExeEnv.execute();

    }

}

class Result{

    private String schema_type;
    private long c2s_byte_num;
    private long c2s_pkt_num;
    private long s2c_byte_num;
    private long s2c_pkt_num;
    private long common_recv_time;
    private byte[] ip_object;
    private byte[] itemsSketch;
    private byte[] thetaSketch;

    public String getSchema_type() {
        return schema_type;
    }

    public void setSchema_type(String schema_type) {
        this.schema_type = schema_type;
    }

    public long getC2s_byte_num() {
        return c2s_byte_num;
    }

    public void setC2s_byte_num(long c2s_byte_num) {
        this.c2s_byte_num = c2s_byte_num;
    }

    public long getC2s_pkt_num() {
        return c2s_pkt_num;
    }

    public void setC2s_pkt_num(long c2s_pkt_num) {
        this.c2s_pkt_num = c2s_pkt_num;
    }

    public long getS2c_byte_num() {
        return s2c_byte_num;
    }

    public void setS2c_byte_num(long s2c_byte_num) {
        this.s2c_byte_num = s2c_byte_num;
    }

    public long getS2c_pkt_num() {
        return s2c_pkt_num;
    }

    public void setS2c_pkt_num(long s2c_pkt_num) {
        this.s2c_pkt_num = s2c_pkt_num;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public byte[] getIp_object() {
        return ip_object;
    }

    public void setIp_object(byte[] ip_object) {
        this.ip_object = ip_object;
    }

    public byte[] getItemsSketch() {
        return itemsSketch;
    }

    public void setItemsSketch(byte[] itemsSketch) {
        this.itemsSketch = itemsSketch;
    }

    public byte[] getThetaSketch() {
        return thetaSketch;
    }

    public void setThetaSketch(byte[] thetaSketch) {
        this.thetaSketch = thetaSketch;
    }

    @Override
    public String toString() {
        return "Result{" +
                "schema_type='" + schema_type + '\'' +
                ", c2s_byte_num=" + c2s_byte_num +
                ", c2s_pkt_num=" + c2s_pkt_num +
                ", s2c_byte_num=" + s2c_byte_num +
                ", s2c_pkt_num=" + s2c_pkt_num +
                ", common_recv_time=" + common_recv_time +
                ", ip_object='" + ip_object + '\'' +
                '}';
    }
}
