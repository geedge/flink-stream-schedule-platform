package com.zdjizhi.etl.utils;

import cn.hutool.crypto.digest.DigestUtil;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.base.utils.HttpClientUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class KnowledgeUtils {

    private static String KNOWLEDGE_EXPR = null;
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static Map<String, String> knowLedgeFileSha256 = new HashMap<>();
    private static Map<String, String> ipTagKnowLedgeFileSha256 = new HashMap<>();
    private static Map<String, String> domainTagKnowLedgeFileSha256 = new HashMap<>();
    private static Map<String, String> appTagKnowLedgeFileSha256 = new HashMap<>();

    private static boolean isFirst = true;


    static {
        buildExpr();
    }

    public static void processNacosInfo(String nacosInfo) {
        String currentDate = formatter.format(new Date());
        System.out.println(currentDate + ": receive nacos config:" + nacosInfo);
        ArrayList<Map<String, Object>> metaList = JsonPath.parse(nacosInfo).read(KNOWLEDGE_EXPR);
        System.out.println("resolve " + metaList.size() + " metadata from the config info pushed by nacos");
        loadKnowledge(metaList);
    }

    private static void checkMetaList(ArrayList<Map<String, Object>> metaList) {
        List<String> types = new ArrayList<>(metaList.size());
        for (Map<String, Object> metadata : metaList) {
            String type = (String) metadata.get("type");
            types.add(type);
        }
        check(types, CommonInternalConfig.IPV4_TYPE);
        check(types, CommonInternalConfig.IPV6_TYPE);
        check(types, CommonInternalConfig.ASNV4_TYPE);
        check(types, CommonInternalConfig.ASNV6_TYPE);
        check(types, CommonInternalConfig.WEBSKT_TYPE);
        check(types, CommonInternalConfig.DNS_TYPE);
        check(types, CommonInternalConfig.ICP_TYPE);
        check(types, CommonInternalConfig.LINK_TYPE);
        check(types, CommonInternalConfig.IDC_RENTER_TYPE);
        check(types, CommonInternalConfig.INTERNAL_IP_TYPE);
        check(types, CommonInternalConfig.APPSKT_TYPE);
        check(types, CommonInternalConfig.WHOIS_TYPE);
        check(types, CommonInternalConfig.PSIPHON_TYPE);
        check(types, CommonInternalConfig.IOC_DARKWEB_TYPE);
        check(types, CommonInternalConfig.IOC_MALWARE_TYPE);
    }

    private static void check(List<String> types, String type) {
        if (!types.contains(type)) {
            throw new RuntimeException("can not find type :" + type + " from knowledge database");
        }
    }

    private static void loadKnowledge(ArrayList<Map<String, Object>> metaList) {
        List<String> tagTypes = new ArrayList<>(3);
        tagTypes.add(CommonInternalConfig.IP_TAG_TYPE);
        tagTypes.add(CommonInternalConfig.DOMAIN_TAG_TYPE);
        tagTypes.add(CommonInternalConfig.APP_TAG_TYPE);

        Map<String, String> ipTagMap = new HashMap<>();
        Map<String, String> domainTagMap = new HashMap<>();
        Map<String, String> appTagMap = new HashMap<>();

        List<String> ipInvalid = new ArrayList<>();
        List<String> domainInvalid = new ArrayList<>();
        List<String> appInvalid = new ArrayList<>();

        Map<String, String> ipTagUrl = new HashMap<>();
        Map<String, String> domainTagUrl = new HashMap<>();
        Map<String, String> appTagUrl = new HashMap<>();

        try {
            //checkMetaList(metaList);
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
                    String fileName = name + "." + format;
                    if (CommonInternalConfig.IP_TAG_TYPE.equals(type)) {
                        ipTagMap.put(fileName, sha256);
                        ipTagUrl.put(fileName, fileUrl);
                        if (isValid != 1) {
                            ipInvalid.add(name);
                        }
                    }
                    if (CommonInternalConfig.DOMAIN_TAG_TYPE.equals(type)) {
                        domainTagMap.put(fileName, sha256);
                        domainTagUrl.put(fileName, fileUrl);
                        if (isValid != 1) {
                            domainInvalid.add(name);
                        }
                    }
                    if (CommonInternalConfig.APP_TAG_TYPE.equals(type)) {
                        appTagMap.put(fileName, sha256);
                        appTagUrl.put(fileName, fileUrl);
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
                Map<String, byte[]> contents = new HashMap<>(ipTagMap.size());
                for (String s : ipTagMap.keySet()) {
                    byte[] downloadBytes = downloadFile(ipTagUrl.get(s), ipTagMap.get(s), 1);
                    contents.put(s, downloadBytes);
                }
                TagUtils.updateIpTag(contents);
            }
            if (!checkSha256(domainTagKnowLedgeFileSha256, domainTagMap)) {
                domainTagKnowLedgeFileSha256 = domainTagMap;
                Map<String, byte[]> contents = new HashMap<>(domainTagMap.size());
                for (String s : domainTagMap.keySet()) {
                    byte[] downloadBytes = downloadFile(domainTagUrl.get(s), domainTagMap.get(s), 1);
                    contents.put(s, downloadBytes);
                }
                TagUtils.updateDomainTag(contents);
            }
            if (!checkSha256(appTagKnowLedgeFileSha256, appTagMap)) {
                appTagKnowLedgeFileSha256 = appTagMap;
                Map<String, byte[]> contents = new HashMap<>(appTagMap.size());
                for (String s : appTagMap.keySet()) {
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

    private static boolean checkSha256(Map<String, String> map1, Map<String, String> map2) {
        if (map1 == null || map1.size() == 0) {
            return map2 == null || map2.size() == 0;
        } else if (map1.size() != map2.size()) {
            return false;
        } else {
            for (String s : map1.keySet()) {
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

    private static void updateKnowledge(String type, String format, byte[] contents) throws IOException {
        /*if (contents == null || contents.length == 0) {
            throw new RuntimeException("download knowledge database error, type is :" + type);
        }*/
        long start = System.currentTimeMillis();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(contents);
        if ("mmdb".equals(format)) {
            //IPUtils.update(type, inputStream);
            IPUtils.init(type, inputStream);
        }
        if ("csv".equals(format)) {
            InputStreamReader reader = new InputStreamReader(inputStream);
            if ((CommonInternalConfig.APPSKT_TYPE).equals(type)) {
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
            if ((CommonInternalConfig.WHOIS_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("whois_registrant_org");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                WhoisUtils.readCsv(highCsvReader);
            }
            if ((CommonInternalConfig.WEBSKT_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("reputation_level");
                needColumns.add("category_name");
                needColumns.add("category_group");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                FcUtils.readCsv(highCsvReader);
            }
            if ((CommonInternalConfig.DNS_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("ip_addr");
                needColumns.add("dns_server_role");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                DnsServerUtils.readServerCsv(highCsvReader);
            }
            if ((CommonInternalConfig.ICP_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("icp_company_name");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                FcUtils.readIcpCsv(highCsvReader);
            }
            if ((CommonInternalConfig.LINK_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("peer_city");
                needColumns.add("link_id");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                LinkUtils.readLinkCsv(highCsvReader);
            }
            if ((CommonInternalConfig.IDC_RENTER_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                needColumns.add("idc_renter");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IdcRenterUtils.readServerCsv(highCsvReader);
            }
            if ((CommonInternalConfig.INTERNAL_IP_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IPUtils.readInternalIpCsv(highCsvReader);
            }
            if ((CommonInternalConfig.PSIPHON_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                needColumns.add("type");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                PsiphonUtils.readPsiphonCsv(highCsvReader);
            }
            if ((CommonInternalConfig.IOC_DARKWEB_TYPE).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("ioc_type");
                needColumns.add("ioc_value");
                needColumns.add("node_type");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IocDarkWebUtils.readIocDarkWebCsv(highCsvReader);
            }
            if ((CommonInternalConfig.IOC_MALWARE_TYPE).equals(type)) {
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

    private static byte[] downloadFile(String fileUrl) {
        byte[] content = new byte[0];
        try {
            long start = System.currentTimeMillis();
            Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            content = httpClientUtils.httpGetInputStream(fileUrl, header);
            long end = System.currentTimeMillis();
            System.out.println("download file " + fileUrl + " finished, speed " + (end - start) + " ms");
        } catch (Exception e) {
            System.out.println("download file " + fileUrl + " error: " + e.getMessage());
        }
        return content;
    }

    private static byte[] downloadFile(String fileUrl, String sha256, int isValid) {
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
                System.out.println("warning: file url" + fileUrl + " download failed, download size : " + content.length + ", download sha256:" + downloadFileSha256 + ", hope sha256 is " + sha256);
            }
        } while (!downloadFileSha256.equals(sha256) && downloadCount < CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER);
        if (downloadCount >= CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER) {
            System.out.println("warning: file url: " + fileUrl + " download more than specified number of times");
        }
        return content;
    }

    private static void buildExpr() {
        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.type in ['");
        sb.append(CommonInternalConfig.IPV4_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IPV6_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.ASNV4_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.ASNV6_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.WEBSKT_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.DNS_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.ICP_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.LINK_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IDC_RENTER_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.INTERNAL_IP_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.APPSKT_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.WHOIS_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IP_TAG_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.DOMAIN_TAG_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.APP_TAG_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.PSIPHON_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IOC_DARKWEB_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IOC_MALWARE_TYPE);
        sb.append("'])].['name','sha256','format','path','type','isValid']");
        KNOWLEDGE_EXPR = sb.toString();
    }

    public static void loadFromLocal() throws IOException {
        updateKnowledge(CommonInternalConfig.IPV4_TYPE, "mmdb", FileUtils.getFileBytes(CommonInternalConfig.IPV4_TYPE + ".mmdb"));
        updateKnowledge(CommonInternalConfig.IPV6_TYPE, "mmdb", FileUtils.getFileBytes(CommonInternalConfig.IPV6_TYPE + ".mmdb"));
        updateKnowledge(CommonInternalConfig.ASNV4_TYPE, "mmdb", FileUtils.getFileBytes(CommonInternalConfig.ASNV4_TYPE + ".mmdb"));
        updateKnowledge(CommonInternalConfig.ASNV6_TYPE, "mmdb", FileUtils.getFileBytes(CommonInternalConfig.ASNV6_TYPE + ".mmdb"));
        updateKnowledge(CommonInternalConfig.WEBSKT_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.WEBSKT_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.DNS_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.DNS_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.ICP_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.ICP_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.LINK_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.LINK_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.IDC_RENTER_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.IDC_RENTER_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.INTERNAL_IP_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.INTERNAL_IP_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.APPSKT_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.APPSKT_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.WHOIS_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.WHOIS_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.PSIPHON_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.PSIPHON_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.IOC_DARKWEB_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.IOC_DARKWEB_TYPE + ".csv"));
        updateKnowledge(CommonInternalConfig.IOC_MALWARE_TYPE, "csv", FileUtils.getFileBytes(CommonInternalConfig.IOC_MALWARE_TYPE + ".csv"));
    }
}
