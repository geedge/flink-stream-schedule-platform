//package com.zdjizhi.base.utils;
//
//
//import com.alibaba.nacos.api.PropertyKeyConst;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.maxmind.db.CHMCache;
//import com.maxmind.db.Reader;
//import com.zdjizhi.base.common.CustomFile;
//import com.zdjizhi.base.connectors.http.HttpSource;
//import com.zdjizhi.utils.GalaxyDataBaseReader;
//import com.zdjizhi.utils.IpLookupV2;
//import org.apache.flink.api.common.state.BroadcastState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
//import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.test.util.MiniClusterWithClientResource;
//
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.IOUtils;
//import org.junit.ClassRule;
//import org.junit.Test;
//
//import java.io.ByteArrayInputStream;
//import java.io.InputStream;
//import java.net.InetAddress;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//public class FlinkJobTest {
//
//    @ClassRule
//    public static MiniClusterWithClientResource flinkCluster =
//            new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder()
//                            .setNumberSlotsPerTaskManager(2)
//                            .setNumberTaskManagers(1)
//                            .build()
//            );
//
//    @Test
//    public void jobTest() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        Properties nacosProperties = new Properties();
//
//        String STORE_PATH = "/test/TEST/";
//
//        nacosProperties.put(PropertyKeyConst.SERVER_ADDR,"192.168.44.12");
//        //nacosProperties.setProperty(PropertyKeyConst.NAMESPACE, NACOS_NAMESPACE);
//        nacosProperties.setProperty(PropertyKeyConst.USERNAME, "nacos");
//        nacosProperties.setProperty(PropertyKeyConst.PASSWORD, "nacos");
//        DataStreamSource<List<CustomFile>> customFileDataStreamSource = env.addSource(new HttpSource(nacosProperties, "knowledge_base.json", "DEFAULT_GROUP", 30000,STORE_PATH));
//        customFileDataStreamSource.setParallelism(1);
//
//        MapStateDescriptor<String,List<CustomFile>> descriptor =
//                new MapStateDescriptor<String, List<CustomFile>>("descriptorTest", Types.STRING, Types.LIST(TypeInformation.of(CustomFile.class)));
//        BroadcastStream<List<CustomFile>> broadcast = customFileDataStreamSource.broadcast(descriptor);
//
//        BroadcastConnectedStream<String, List<CustomFile>> connect = env.addSource(new MySource()).setParallelism(1).connect(broadcast);
//
//        connect.process(new BroadcastProcessFunction<String, List<CustomFile>, Tuple2<String,String>>() {
//            Reader reader = null;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                System.out.println("begin init");
//                byte[] fileBytes = HdfsUtils.getFileBytes(STORE_PATH + "ip_v4_built_in.mmdb");
//                InputStream inputStream = new ByteArrayInputStream(fileBytes);
//                this.reader = new Reader(inputStream,new CHMCache());
//                System.out.println("init over");
//            }
//
//            @Override
//            public void processElement(String value, BroadcastProcessFunction<String, List<CustomFile>, Tuple2<String, String>>.ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
//                //System.out.println("receive vaule:" + value);
//                InetAddress ipAddress = InetAddress.getByName(value);
//                String result = "ip not find";
//                JsonNode jsonNode = reader.get(ipAddress);
//                if (jsonNode != null) {
//                    result = jsonNode.toString();
//                }
//                out.collect(Tuple2.of(value,result));
//            }
//
//            @Override
//            public void processBroadcastElement(List<CustomFile> value, BroadcastProcessFunction<String, List<CustomFile>, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
//                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                String format = formatter.format(new Date());
//                System.out.println(format + "receive broadcast value");
//                BroadcastState<String, List<CustomFile>> broadcastState = ctx.getBroadcastState(descriptor);
//                broadcastState.clear();
//                broadcastState.put("test",value);
//                InputStream inputStream = null;
//                for (CustomFile customFile : value) {
//                    if ("ip_v4_built_in.mmdb".equals(customFile.getFileName())) {
//                        inputStream = new ByteArrayInputStream(customFile.getContent());
//                    }
//                }
//                String format1 = formatter.format(new Date());
//                System.out.println(format1 + "begin update reader");
//                this.reader = new Reader(inputStream, new CHMCache());
//            }
//        }).setParallelism(1).addSink(new CollectSink1());
//
//        env.execute();
//
//        for (Tuple2<String, String> value : CollectSink1.values) {
//            System.out.println(value);
//        }
//        System.out.println(CollectSink1.values);
//    }
//
//    public static class CollectSink implements SinkFunction<CustomFile> {
//
//        public static final List<CustomFile> values = Collections.synchronizedList(new ArrayList<CustomFile>());
//
//        public void invoke(CustomFile value, Context context) throws Exception {
//            values.add(value);
//        }
//    }
//
//    public static class CollectSink1 implements SinkFunction<Tuple2<String,String>> {
//
//        public static final List<Tuple2<String,String>> values = Collections.synchronizedList(new ArrayList<Tuple2<String,String>>());
//
//        public void invoke(Tuple2<String,String> value, Context context) throws Exception {
//            System.out.println(value.f0 + " | " + value.f1);
//            values.add(value);
//        }
//    }
//
//    public static class MySource implements SourceFunction<String> {
//
//        private boolean isRunning = true;
//
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//            System.out.println("MySource begin");
//            int count = 0;
//            while (isRunning) {
//                Random r = new Random();
//                String randomIp = r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255);
//                ctx.collect(randomIp);
//                Thread.sleep(1000);
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//        }
//    }
//
//}
