package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.utils.IpLookup;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class DnsUtils {
    private static Logger LOG = LoggerFactory.getLogger(DnsUtils.class);
    private static HashMap<String, CnRecordLog> dnsmap = readServerCsv(CommonConfig.DNS_PATH);

    private static HashMap<String, CnRecordLog> readServerCsv(String path) {
        HashMap<String, CnRecordLog> fcmap = new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[1];
                ArrayList<String> serverRole = new ArrayList<>();
                if (!key.startsWith("#")) {
                    CnRecordLog catalog = new CnRecordLog();
                    catalog.setCommon_server_ip(key);
                    serverRole.add(strs[6]);
                    catalog.setDns_server_role(serverRole);
                    catalog.setDns_server_org(strs[7]);
                    catalog.setDns_server_software(strs[9]);
                    catalog.setDns_server_os(strs[8]);
                    fcmap.put(key, catalog);
                    System.out.println(catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return fcmap;
    }

    public static void getWebSktMessage(CnRecordLog cnRecordLog) {
        try {
            if (dnsmap.containsKey(cnRecordLog.getCommon_server_ip())) {
                cnRecordLog.setDns_server_role(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_role());
                cnRecordLog.setDns_server_org(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_org());
                cnRecordLog.setDns_server_os(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_os());
                cnRecordLog.setDns_server_software(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_software());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }


    public static void main(String[] args) throws IOException {
        HashMap<String, CnRecordLog> map = readServerCsv(CommonConfig.DNS_PATH);
        Set<String> keySet = map.keySet();
        //Low Level Client init
//        String[] split = CommonConfig.ES_HOST.split(":");
//        RestClientBuilder lowLevelRestClient = RestClient.builder(
//                new HttpHost(split[0], Integer.parseInt(split[split.length - 1]), "http"));

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        //High Level Client init
        RestHighLevelClient client =
                new RestHighLevelClient(lowLevelRestClient);
        IpLookup ipLookup = IPUtils.ipLookup;
        for (String key : keySet) {
            CnRecordLog cnRecordLog = map.get(key);
            System.out.println(cnRecordLog.toString());
            String serverIp = cnRecordLog.getCommon_server_ip();
            IndexRequest source = new IndexRequest("cn_entity")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("id", serverIp)
                            .field("entity_type", "ip")
                            .field("found_time", System.currentTimeMillis() / 1000)
                            .field("update_time", System.currentTimeMillis() / 1000)
                            .field("ip_addr", serverIp)
                            .field("ip_location_country", ipLookup.countryLookup(serverIp))
                            .field("ip_location_province",ipLookup.provinceLookup(serverIp))
                            .field("ip_location_city",ipLookup.cityLookup(serverIp))
                            .field("ip_asn",ipLookup.asnLookup(serverIp))
                            .field("dns_server_role", cnRecordLog.getDns_server_role())
                            .field("dns_server_org", cnRecordLog.getDns_server_org())
                            .field("dns_server_os", cnRecordLog.getDns_server_os())
                            .field("dns_server_software", cnRecordLog.getDns_server_software())
                            .field("doh_support", cnRecordLog.getDoh_support() == 1)
                            .field("dot_support", cnRecordLog.getDot_support() == 1)
                            .field("dnssec_support", false)
                            .endObject()
                    )
                    .id(serverIp);
            client.index(source, RequestOptions.DEFAULT);
        }
        client.close();
        System.out.println(map.size());
    }
}

