package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.etl.common.DnsServerInfo;
import com.zdjizhi.etl.common.LinkAndIspInfo;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
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
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class DnsServerUtils {
    private static Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static HashMap<String, DnsServerInfo> dnsmap = new HashMap<>();

    public static void readServerCsv(Reader serverReader) {

        try {
            CSVReader csvReader = new CSVReader(serverReader, CSVParser.DEFAULT_SEPARATOR,
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
                    String dnssec = strs[10];
                    if (StringUtil.isNotBlank(dnssec)) {
                        catalog.setDnssecSupport(Integer.parseInt(dnssec));
                    }
                    String dot = strs[11];
                    if (StringUtil.isNotBlank(dot)) {
                        catalog.setDotSupport(Integer.parseInt(dot));
                    }
                    String doh = strs[12];
                    if (StringUtil.isNotBlank(doh)) {
                        catalog.setDohSupport(Integer.parseInt(doh));
                    }
                    catalog.setRootMirrorLocations(strs[13]);
                    dnsmap.put(key, catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error("加载失败", e);
        }
    }

    public static void setWebSktMessage(CnRecordLog cnRecordLog) {
        String commonServerIp = cnRecordLog.getCommon_server_ip();
        ArrayList<String> dnsServerRoles = new ArrayList<>();
        if (cnRecordLog.getDns_aa() == 1) {
            dnsServerRoles.add("ADNS");
        }
        if (dnsmap.containsKey(commonServerIp)) {
            DnsServerInfo dnsServerInfo = dnsmap.get(commonServerIp);
            String dnsServerRole = dnsServerInfo.getDnsServerRole();
            dnsServerRoles.add(dnsServerRole);
            cnRecordLog.setDns_server_org(dnsServerInfo.getOrgnization());
            cnRecordLog.setDns_server_os(dnsServerInfo.getOperationSystem());
            cnRecordLog.setDns_server_software(dnsServerInfo.getDnsSoftware());
            if (cnRecordLog.getCommon_server_port() == 443 && dnsServerInfo.getDohSupport() == 1) {
                cnRecordLog.setDns_protocol("doh");
            } else if (cnRecordLog.getCommon_server_port() == 853 && dnsServerInfo.getDotSupport() == 1) {
                cnRecordLog.setDns_protocol("dot");
            } else if (cnRecordLog.getDns_sub() == 2) {
                cnRecordLog.setDns_protocol("dnssec");
            }
        }
        cnRecordLog.setDns_server_role(dnsServerRoles);
    }

    public static void main(String[] args) throws IOException {
        String dnsPath = CommonConfig.DNS_PATH;
        System.out.println(dnsPath);
        HashMap<String, DnsServerInfo> map = dnsmap;
        Set<String> keySet = map.keySet();

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(lowLevelRestClient);
        IpLookupV2 ipLookup = IPUtils.ipLookup;
        HashMap<String, LinkAndIspInfo> ispMap = IspUtils.ispmap;
        for (String key : keySet) {
            DnsServerInfo dnsServerInfo = map.get(key);
            try {
                String serverIp = dnsServerInfo.getIpAddr();
                IPAddress ipAddress = new IPAddressString(serverIp).getAddress();
                IPAddress mask = ipAddress.getNetwork().getNetworkMask(24, false);
                String masked = ipAddress.mask(mask).toString();
                LinkAndIspInfo ispInfo = ispMap.getOrDefault(masked, null);
                String country;
                String province;
                String city;
                String isp = StringUtil.EMPTY;
                if (ispInfo != null) {
                    isp = "CUCC";
                    country = "China";
                    province = "Xinjiang";
                    city = ispInfo.getRegion();
                } else {
                    country = ipLookup.countryLookup(serverIp);
                    province = ipLookup.provinceLookup(serverIp);
                    city = ipLookup.cityLookup(serverIp);
                }
                IndexRequest source = new IndexRequest("entity_info")
                        .source(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("id", serverIp)
                                .field("entity_type", "ip")
                                .field("found_time", System.currentTimeMillis() / 1000)
                                .field("update_time", System.currentTimeMillis() / 1000)
                                .field("ip_addr", serverIp)
                                .field("ip_location_country", country)
                                .field("ip_location_province", province)
                                .field("ip_location_city", city)
                                .field("ip_asn", ipLookup.asnLookup(serverIp))
                                .field("ip_isp", isp)
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
            } catch (Exception e) {
                LOG.error("写入{}失败", dnsServerInfo);
            }
        }
        client.close();
        System.out.println(map.size());
    }
}

