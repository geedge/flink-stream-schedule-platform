package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.ESSinkUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DnsServerTest {
    private static Logger LOG = LoggerFactory.getLogger(DnsServerTest.class);

    public static void main(String[] args) throws Exception {
        switch (CommonConfig.CN_RECORD_PARALLELISM) {
            case 1:
                setDnsByOprdns();
                break;
            case 2:
                setDnsByDoh();
                break;
            case 3:
                setdomain();
                break;
            default:
                throw new RuntimeException();
        }

    }

    public static void setDnsByOprdns() throws Exception {
        DataInputStream in = new DataInputStream(new FileInputStream(new File("D:\\data\\Public DNS Server List.txt")));
//        DataInputStream in = new DataInputStream(new FileInputStream(new File(CommonConfig.WEBSKT_PATH)));
        CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

//        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses("192.168.40.73:9200");
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(lowLevelRestClient);

        BulkRequest bulkRequest = new BulkRequest();
        int i = 0;

        for (String[] strs : csvReader) {
            i += 1;
            String key = strs[0];
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("id", key)
                    .field("entity_type", "ip")
                    .field("found_time", System.currentTimeMillis() / 1000)
                    .field("update_time", System.currentTimeMillis() / 1000)
                    .field("ip_addr", key)
                    .field("dns_server_role", "OPRDNS")
                    .field("doh_support", false)
                    .endObject();
            UpdateRequest source = new UpdateRequest("entity_info", key)
                    .doc(builder)
                    .upsert(builder)
                    .id(key);
            bulkRequest.add(source);

            if (i >= com.zdjizhi.etl.common.CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN) {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOG.info("写入{}", i);
                i = 0;
            }
        }
        csvReader.close();

        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        LOG.info("写入完毕");
        client.close();

    }

    public static void setDnsByDoh() throws Exception {
        DataInputStream in = new DataInputStream(new FileInputStream(new File("D:\\data\\Public DoH Server List.txt")));
//        DataInputStream in = new DataInputStream(new FileInputStream(new File(CommonConfig.WEBSKT_PATH)));
        CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

//        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses("192.168.40.73:9200");
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(lowLevelRestClient);

        BulkRequest bulkRequest = new BulkRequest();
        int i = 0;

        for (String[] strs : csvReader) {
            i += 1;
            String key = strs[0];
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("id", key)
                    .field("entity_type", "ip")
                    .field("found_time", System.currentTimeMillis() / 1000)
                    .field("update_time", System.currentTimeMillis() / 1000)
                    .field("ip_addr", key)
                    .field("dns_server_role", "OPRDNS")
                    .field("doh_support", true)
                    .endObject();
            UpdateRequest source = new UpdateRequest("entity_info", key)
                    .doc(builder)
                    .upsert(builder)
                    .id(key);
            bulkRequest.add(source);

            if (i >= com.zdjizhi.etl.common.CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN) {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOG.info("写入{}", i);
                i = 0;
            }
        }
        csvReader.close();

        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        LOG.info("写入完毕");
        client.close();

    }

    public static void setdomain() throws Exception {
//        DataInputStream in = new DataInputStream(new FileInputStream(new File("D:\\data\\tmp\\domains.csv.ag")));
        DataInputStream in = new DataInputStream(new FileInputStream(new File(CommonConfig.WEBSKT_PATH)));
        CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

//        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses("192.168.40.73:9200");
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(lowLevelRestClient);

        BulkRequest bulkRequest = new BulkRequest();
        int i = 0;

        for (String[] strs : csvReader) {
            i += 1;
            String key = strs[0];
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("id", key)
                    .field("entity_type", "domain")
                    .field("found_time", System.currentTimeMillis() / 1000)
                    .field("update_time", System.currentTimeMillis() / 1000)
                    .field("domain_name", key)
                    .endObject();
            UpdateRequest source = new UpdateRequest("entity_info", key)
                    .doc(builder)
                    .upsert(builder)
                    .id(key);
            bulkRequest.add(source);

            if (i >= com.zdjizhi.etl.common.CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN) {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOG.info("写入{}", i);
                i = 0;
            }
        }
        csvReader.close();

        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        LOG.info("写入完毕");
        client.close();

    }


}
