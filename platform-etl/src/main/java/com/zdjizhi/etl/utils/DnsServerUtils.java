package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.etl.common.DnsServerInfo;
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
public class DnsServerUtils {
    private static Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static HashMap<String, DnsServerInfo> dnsmap = readServerCsv(CommonConfig.DNS_PATH);

    private static HashMap<String, DnsServerInfo> readServerCsv(String path) {
        HashMap<String, DnsServerInfo> fcmap = new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[1];
                if (!key.startsWith("#")) {
                    DnsServerInfo catalog = new DnsServerInfo();
                    catalog.setIpAddr(key);
                    catalog.setDnsServerRole(strs[6]);
                    catalog.setOrgnization(strs[7]);
                    catalog.setDnsSoftware(strs[9]);
                    catalog.setOperationSystem(strs[8]);
                    catalog.setDnssecSupport(Integer.parseInt(strs[10]));
                    catalog.setDotSupport(Integer.parseInt(strs[11]));
                    catalog.setDohSupport(Integer.parseInt(strs[12]));
                    catalog.setRootMirrorLocations(strs[13]);
                    fcmap.put(key, catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return fcmap;
    }

    public static void getWebSktMessage(Entity entity, CnRecordLog cnRecordLog) {
        try {
            String commonServerIp = entity.getCommon_server_ip();
            if (dnsmap.containsKey(commonServerIp)) {
                DnsServerInfo dnsServerInfo = dnsmap.get(commonServerIp);
                ArrayList<String> dnsServerRoles = new ArrayList<>();
                String dnsServerRole = dnsServerInfo.getDnsServerRole();
                dnsServerRoles.add(dnsServerRole);
                if (entity.getDns_aa() == 1) {
                    dnsServerRoles.add("ADNS");
                }
                cnRecordLog.setDns_server_role(dnsServerRoles);
                cnRecordLog.setDns_server_org(dnsServerInfo.getOrgnization());
                cnRecordLog.setDns_server_os(dnsServerInfo.getOperationSystem());
                cnRecordLog.setDns_server_software(dnsServerInfo.getDnsSoftware());
                if ("DNS".equals(entity.getCommon_l7_protocol())) {
                    if (entity.getCommon_server_port() == 443 && dnsServerInfo.getDohSupport() == 1) {
                        cnRecordLog.setDns_protocol("doh");
                    } else if (entity.getCommon_server_port() == 853 && dnsServerInfo.getDotSupport() == 1) {
                        cnRecordLog.setDns_protocol("dot");
                    } else if (entity.getDns_sub() == 1) {
                        cnRecordLog.setDns_protocol("dnssec");
                    } else {
                        cnRecordLog.setDns_protocol("dns");
                    }
                } else {
                    cnRecordLog.setDns_protocol("");
                }
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }


    public static void main(String[] args) throws IOException {
        HashMap<String, DnsServerInfo> map = readServerCsv(CommonConfig.DNS_PATH);
        Set<String> keySet = map.keySet();

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(lowLevelRestClient);
        IpLookup ipLookup = IPUtils.ipLookup;
        for (String key : keySet) {
            DnsServerInfo dnsServerInfo = map.get(key);
            String serverIp = dnsServerInfo.getIpAddr();
            IndexRequest source = new IndexRequest("cn_entity")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("id", serverIp)
                            .field("entity_type", "ip")
                            .field("found_time", System.currentTimeMillis() / 1000)
                            .field("update_time", System.currentTimeMillis() / 1000)
                            .field("ip_addr", serverIp)
                            .field("ip_location_country", ipLookup.countryLookup(serverIp))
                            .field("ip_location_province", ipLookup.provinceLookup(serverIp))
                            .field("ip_location_city", ipLookup.cityLookup(serverIp))
                            .field("ip_asn", ipLookup.asnLookup(serverIp))
                            .field("dns_server_role", dnsServerInfo.getDnsServerRole())
                            .field("dns_server_org", dnsServerInfo.getOrgnization())
                            .field("dns_server_os", dnsServerInfo.getOperationSystem())
                            .field("dns_server_software", dnsServerInfo.getDnsSoftware())
                            .field("dns_root_mirror_locations", dnsServerInfo.getRootMirrorLocations())
                            .field("doh_support", dnsServerInfo.getDohSupport() == 1)
                            .field("dot_support", dnsServerInfo.getDotSupport() == 1)
                            .field("dnssec_support", dnsServerInfo.getDnssecSupport() == 1)
                            .endObject()
                    )
                    .id(serverIp);
            client.index(source, RequestOptions.DEFAULT);
        }
        client.close();
        System.out.println(map.size());
    }
}

