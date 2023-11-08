package com.zdjizhi.etl.knowledge;

import cn.hutool.crypto.digest.DigestUtil;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.base.utils.HttpClientUtils;
import com.zdjizhi.etl.utils.*;
import org.apache.flink.configuration.Configuration;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class KnowledgeManager {

    public Configuration configuration;
    private final String KNOWLEDGE_EXPR;

    private Map<String, String> knowLedgeFileSha256 = new HashMap<>();
    private Map<Long, String> ipTagKnowLedgeFileSha256 = new HashMap<>();
    private Map<Long, String> domainTagKnowLedgeFileSha256 = new HashMap<>();
    private Map<Long, String> appTagKnowLedgeFileSha256 = new HashMap<>();

    private boolean isFirst = true;

    public KnowledgeManager(Configuration configuration) {
        this.configuration = configuration;
        this.KNOWLEDGE_EXPR = buildExpr();
    }

    public void processNacosInfo(String nacosInfo) {
        System.out.println(TimeUtils.getCurrentTime() + ": receive nacos config:" + nacosInfo);
        ArrayList<Map<String, Object>> metaList = JsonPath.parse(nacosInfo).read(KNOWLEDGE_EXPR);
        System.out.println("resolve " + metaList.size() + " metadata from the config info pushed by nacos");
        loadKnowledge(metaList);
    }

    private void checkMetaList(ArrayList<Map<String, Object>> metaList) {
        List<String> types = new ArrayList<>(metaList.size());
        for (Map<String, Object> metadata : metaList) {
            String type = (String) metadata.get("type");
            types.add(type);
        }
        check(types, configuration.get(CommonInternalConfig.IPV4_TYPE));
        check(types, configuration.get(CommonInternalConfig.IPV6_TYPE));
        check(types, configuration.get(CommonInternalConfig.ASNV4_TYPE));
        check(types, configuration.get(CommonInternalConfig.ASNV6_TYPE));
        check(types, configuration.get(CommonInternalConfig.WEBSKT_TYPE));
        check(types, configuration.get(CommonInternalConfig.DNS_TYPE));
        check(types, configuration.get(CommonInternalConfig.ICP_TYPE));
        check(types, configuration.get(CommonInternalConfig.LINK_TYPE));
        check(types, configuration.get(CommonInternalConfig.IDC_RENTER_TYPE));
        check(types, configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE));
        check(types, configuration.get(CommonInternalConfig.APPSKT_TYPE));
        check(types, configuration.get(CommonInternalConfig.WHOIS_TYPE));
        check(types, configuration.get(CommonInternalConfig.PSIPHON_TYPE));
        check(types, configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE));
        check(types, configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE));
    }

    private void check(List<String> types, String type) {
        if (!types.contains(type)) {
            throw new RuntimeException("can not find type :" + type + " from knowledge database");
        }
    }

    private void loadKnowledge(ArrayList<Map<String, Object>> metaList) {
        List<String> tagTypes = new ArrayList<>(3);
        tagTypes.add(configuration.get(CommonInternalConfig.IP_TAG_TYPE));
        tagTypes.add(configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE));
        tagTypes.add(configuration.get(CommonInternalConfig.APP_TAG_TYPE));

        Map<Long, String> ipTagMap = new HashMap<>();
        Map<Long, String> domainTagMap = new HashMap<>();
        Map<Long, String> appTagMap = new HashMap<>();

        List<String> ipInvalid = new ArrayList<>();
        List<String> domainInvalid = new ArrayList<>();
        List<String> appInvalid = new ArrayList<>();

        Map<Long, String> ipTagUrl = new HashMap<>();
        Map<Long, String> domainTagUrl = new HashMap<>();
        Map<Long, String> appTagUrl = new HashMap<>();

        try {
            if (isFirst && configuration.get(CommonInternalConfig.INIT_CHECK) == 1) {
                checkMetaList(metaList);
            }
            for (Map<String, Object> metadata : metaList) {
                String name = (String) metadata.get("name");
                String format = (String) metadata.get("format");
                String sha256 = (String) metadata.get("sha256");
                String fileUrl = (String) metadata.get("path");
                String type = (String) metadata.get("type");
                Integer isValidInteger = (Integer) metadata.get("isValid");
                int isValid = isValidInteger == null ? 1 : isValidInteger;

                if (tagTypes.contains(type)) {
                    if (isValid != 1) {
                        continue;
                    }
                    Long id = Long.parseLong((String) metadata.get("id"));
                    if (configuration.get(CommonInternalConfig.IP_TAG_TYPE).equals(type)) {
                        ipTagMap.put(id, sha256);
                        ipTagUrl.put(id, fileUrl);
                        if (isValid != 1) {
                            ipInvalid.add(name);
                        }
                    }
                    if (configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE).equals(type)) {
                        domainTagMap.put(id, sha256);
                        domainTagUrl.put(id, fileUrl);
                        if (isValid != 1) {
                            domainInvalid.add(name);
                        }
                    }
                    if (configuration.get(CommonInternalConfig.APP_TAG_TYPE).equals(type)) {
                        appTagMap.put(id, sha256);
                        appTagUrl.put(id, fileUrl);
                        if (isValid != 1) {
                            appInvalid.add(name);
                        }
                    }
                } else {
                    String fileName = type + "." + format;
                    if (!knowLedgeFileSha256.containsKey(fileName)) {
                        byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                        updateKnowledge(type, format, downloadBytes);
                    } else if (!knowLedgeFileSha256.get(fileName).equals(sha256)) {
                        byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                        if (downloadBytes.length == 0) {
                            continue;
                        }
                        updateKnowledge(type, format, downloadBytes);
                    }
                    knowLedgeFileSha256.put(fileName, sha256);
                }
            }

            IPUtils.load();

            TagUtils.updateInvalid(ipInvalid, domainInvalid, appInvalid);

            if (!checkSha256(ipTagKnowLedgeFileSha256, ipTagMap)) {
                ipTagKnowLedgeFileSha256 = ipTagMap;
                Map<Long, byte[]> contents = new HashMap<>(ipTagMap.size());
                for (Long s : ipTagMap.keySet()) {
                    byte[] downloadBytes = downloadFile(ipTagUrl.get(s), ipTagMap.get(s), 1);
                    contents.put(s, downloadBytes);
                }
                TagUtils.updateIpTag(contents);
            }
            if (!checkSha256(domainTagKnowLedgeFileSha256, domainTagMap)) {
                domainTagKnowLedgeFileSha256 = domainTagMap;
                Map<Long, byte[]> contents = new HashMap<>(domainTagMap.size());
                for (Long s : domainTagMap.keySet()) {
                    byte[] downloadBytes = downloadFile(domainTagUrl.get(s), domainTagMap.get(s), 1);
                    contents.put(s, downloadBytes);
                }
                TagUtils.updateDomainTag(contents);
            }
            if (!checkSha256(appTagKnowLedgeFileSha256, appTagMap)) {
                appTagKnowLedgeFileSha256 = appTagMap;
                Map<Long, byte[]> contents = new HashMap<>(appTagMap.size());
                for (Long s : appTagMap.keySet()) {
                    byte[] downloadBytes = downloadFile(appTagUrl.get(s), appTagMap.get(s), 1);
                    contents.put(s, downloadBytes);
                }
                TagUtils.updateAppTag(contents);
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
            if (isFirst) {
                throw new RuntimeException("knowledge init fail:" + e.getMessage());
            }
        }
        isFirst = false;
    }

    private boolean checkSha256(Map<Long, String> map1, Map<Long, String> map2) {
        if (map1 == null || map1.size() == 0) {
            return map2 == null || map2.size() == 0;
        } else if (map1.size() != map2.size()) {
            return false;
        } else {
            for (Long s : map1.keySet()) {
                if (!map2.containsKey(s)) {
                    return false;
                } else {
                    if (!map1.get(s).equals(map2.get(s))) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private void updateKnowledge(String type, String format, byte[] contents) throws IOException {
        if (configuration.get(CommonInternalConfig.INIT_CHECK) == 1) {
            if (contents == null || contents.length == 0) {
                if (isFirst) {
                    throw new RuntimeException("download knowledge database error, type is :" + type);
                } else {
                    System.out.println("download knowledge database error, type:" + type + " ignored");
                    return;
                }
            }
        }
        long start = System.currentTimeMillis();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(contents);
        if ("mmdb".equals(format)) {
            //IPUtils.update(type, inputStream);
            IPUtils.init(type, inputStream, configuration);
        }
        if ("csv".equals(format)) {
            InputStreamReader reader = new InputStreamReader(inputStream);
            if ((configuration.get(CommonInternalConfig.APPSKT_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("app_name");
                needColumns.add("app_category");
                needColumns.add("app_subcategory");
                needColumns.add("app_risk");
                needColumns.add("app_is_protocol");
                needColumns.add("app_company");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                AppUtils.readCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.WHOIS_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("whois_registrant_org");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                WhoisUtils.readCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.WEBSKT_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("reputation_level");
                needColumns.add("category_name");
                needColumns.add("category_group");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                FcUtils.readCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.DNS_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("ip_addr");
                needColumns.add("dns_server_role");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                DnsServerUtils.readServerCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.ICP_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("icp_company_name");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                FcUtils.readIcpCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.LINK_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("peer_city");
                needColumns.add("link_id");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                LinkUtils.readLinkCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.IDC_RENTER_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                needColumns.add("idc_renter");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IdcRenterUtils.readServerCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IPUtils.readInternalIpCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.PSIPHON_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                needColumns.add("type");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                PsiphonUtils.readPsiphonCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("ioc_type");
                needColumns.add("ioc_value");
                needColumns.add("node_type");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IocDarkWebUtils.readIocDarkWebCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("ioc_type");
                needColumns.add("ioc_value");
                needColumns.add("malware_name");
                needColumns.add("threat_type");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IocMalwareUtils.readIocMalwareCsv(highCsvReader);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("update " + type + " finished, speed " + (end - start) + " ms");
    }

    private byte[] downloadFile(String fileUrl) {
        byte[] content = new byte[0];
        try {
            long start = System.currentTimeMillis();
            //Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            //content = httpClientUtils.httpGetInputStream(fileUrl, header);
            content = httpClientUtils.httpGetInputStream(fileUrl);
            long end = System.currentTimeMillis();
            System.out.println("download file " + fileUrl + " finished, speed " + (end - start) + " ms");
        } catch (Exception e) {
            System.out.println("download file " + fileUrl + " error: " + e.getMessage());
        }
        return content;
    }

    private byte[] downloadFile(String fileUrl, String sha256, int isValid) {
        byte[] content = new byte[0];
        if (isValid != 1) {
            return content;
        }
        String downloadFileSha256;
        int downloadCount = 0;
        do {
            content = downloadFile(fileUrl);
            downloadFileSha256 = DigestUtil.sha256Hex(content);
            downloadCount++;
            if (!Objects.equals(downloadFileSha256, sha256)) {
                System.out.println("warning: file url: " + fileUrl + " download failed, download size : " + content.length + ", download sha256:" + downloadFileSha256 + ", hope sha256 is " + sha256);
            }
        } while (!downloadFileSha256.equals(sha256) && downloadCount < configuration.get(CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER));
        if (downloadCount >= configuration.get(CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER)) {
            System.out.println("warning: file url: " + fileUrl + " download more than specified number of times");
        }
        return content;
    }

    private String buildExpr() {
        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.type in ['");
        sb.append(configuration.get(CommonInternalConfig.IPV4_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.IPV6_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.ASNV4_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.ASNV6_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.WEBSKT_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.DNS_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.ICP_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.LINK_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.IDC_RENTER_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.APPSKT_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.WHOIS_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.IP_TAG_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.APP_TAG_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.PSIPHON_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE));
        sb.append("'])].['name','sha256','format','path','type','isValid','id']");
        return sb.toString();
    }

    public void loadFromLocal() throws IOException {
        FileUtils fileUtils = new FileUtils(configuration.get(CommonConfig.CLUSTER_OR_SINGLE));
        updateKnowledge(configuration.get(CommonInternalConfig.IPV4_TYPE), "mmdb", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IPV4_TYPE) + ".mmdb"));
        updateKnowledge(configuration.get(CommonInternalConfig.IPV6_TYPE), "mmdb", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IPV6_TYPE) + ".mmdb"));
        updateKnowledge(configuration.get(CommonInternalConfig.ASNV4_TYPE), "mmdb", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.ASNV4_TYPE) + ".mmdb"));
        updateKnowledge(configuration.get(CommonInternalConfig.ASNV6_TYPE), "mmdb", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.ASNV6_TYPE) + ".mmdb"));
        updateKnowledge(configuration.get(CommonInternalConfig.WEBSKT_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.WEBSKT_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.DNS_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.DNS_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.ICP_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.ICP_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.LINK_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.LINK_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IDC_RENTER_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IDC_RENTER_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.APPSKT_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.APPSKT_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.WHOIS_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.WHOIS_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.PSIPHON_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.PSIPHON_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE) + ".csv"));
    }
}
