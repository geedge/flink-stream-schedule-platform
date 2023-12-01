package com.zdjizhi.etl.knowledge;

import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import com.zdjizhi.etl.utils.fqdn.CategoryUtils;
import com.zdjizhi.etl.utils.fqdn.IcpUtils;
import com.zdjizhi.etl.utils.fqdn.WhoisUtils;
import com.zdjizhi.etl.utils.ip.IPUtils;
import com.zdjizhi.etl.utils.ip.InternalIpUtils;
import com.zdjizhi.etl.utils.tag.AppTagUtils;
import com.zdjizhi.etl.utils.tag.DomainTagUtils;
import com.zdjizhi.etl.utils.tag.IpTagUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KnowledgeManager {

    private final Logger logger = LoggerFactory.getLogger(KnowledgeManager.class);

    public Configuration configuration;
    private final String KNOWLEDGE_EXPR;

    private final Map<String, String> knowLedgeFileSha256 = new HashMap<>();

    private CustomKnowledge ipKnowledge = new CustomKnowledge();
    private CustomKnowledge domainKnowledge = new CustomKnowledge();
    private CustomKnowledge appKnowledge = new CustomKnowledge();

    private boolean isFirst = true;

    public KnowledgeManager(Configuration configuration) {
        this.configuration = configuration;
        this.KNOWLEDGE_EXPR = buildExpr();
    }

    public void processNacosInfo(String nacosInfo) {
        logger.info("receive nacos config:" + nacosInfo);
        ArrayList<Map<String, Object>> metaList = JsonPath.parse(nacosInfo).read(KNOWLEDGE_EXPR);
        logger.info("resolve " + metaList.size() + " metadata from the config info pushed by nacos");
        loadKnowledge(metaList);
    }

    private void checkMetaList(ArrayList<Map<String, Object>> metaList) {
        List<String> types = new ArrayList<>(metaList.size());
        for (Map<String, Object> metadata : metaList) {
            String type = (String) metadata.get("type");
            types.add(type);
        }
        check(types, configuration.get(CommonInternalConfig.IP_TYPE));
        check(types, configuration.get(CommonInternalConfig.ASN_TYPE));
        check(types, configuration.get(CommonInternalConfig.FQDN_CATEGORY_TYPE));
        check(types, configuration.get(CommonInternalConfig.DNS_TYPE));
        check(types, configuration.get(CommonInternalConfig.FQDN_ICP_TYPE));
        check(types, configuration.get(CommonInternalConfig.LINK_TYPE));
        check(types, configuration.get(CommonInternalConfig.IDC_RENTER_TYPE));
        check(types, configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE));
        check(types, configuration.get(CommonInternalConfig.APPSKT_TYPE));
        check(types, configuration.get(CommonInternalConfig.FQDN_WHOIS_TYPE));
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

        CustomKnowledge ipKnowledge = new CustomKnowledge();
        CustomKnowledge domainKnowledge = new CustomKnowledge();
        CustomKnowledge appKnowledge = new CustomKnowledge();

        try {
            if (isFirst && configuration.get(CommonInternalConfig.INIT_CHECK) == 1) {
                checkMetaList(metaList);
            }
            Integer checkMax = configuration.get(CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER);
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
                    if (configuration.get(CommonInternalConfig.IP_TAG_TYPE).equals(type)) {
                        ipKnowledge.addMetadata(metadata);
                    } else if (configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE).equals(type)) {
                        domainKnowledge.addMetadata(metadata);
                    } else if (configuration.get(CommonInternalConfig.APP_TAG_TYPE).equals(type)) {
                        appKnowledge.addMetadata(metadata);
                    }
                } else {
                    String fileName = type + "." + format;
                    if (!knowLedgeFileSha256.containsKey(fileName) || !knowLedgeFileSha256.get(fileName).equals(sha256)) {
                        byte[] downloadBytes = CustomKnowledge.downloadFile(fileUrl, sha256, isValid, checkMax);
                        if (downloadBytes.length == 0 && knowLedgeFileSha256.containsKey(fileName)) {
                            continue;
                        }
                        updateKnowledge(type, format, downloadBytes);
                        knowLedgeFileSha256.put(fileName, sha256);
                    }
                }
            }

            IPUtils.load();

            if (!this.ipKnowledge.compare(ipKnowledge)) {
                this.ipKnowledge = ipKnowledge;
                Map<Long, byte[]> contents = ipKnowledge.downloadKnowledge(checkMax);
                IpTagUtils.updateTag(contents);
            }
            if (!this.domainKnowledge.compare(domainKnowledge)) {
                this.domainKnowledge = domainKnowledge;
                Map<Long, byte[]> contents = domainKnowledge.downloadKnowledge(checkMax);
                DomainTagUtils.updateTag(contents);
            }
            if (!this.appKnowledge.compare(appKnowledge)) {
                this.appKnowledge = appKnowledge;
                Map<Long, byte[]> contents = appKnowledge.downloadKnowledge(checkMax);
                AppTagUtils.updateTag(contents);
            }
        } catch (Exception e) {
            logger.error("nacos info load error:" + e.getMessage(), e);
            if (isFirst) {
                throw new RuntimeException("knowledge init fail:" + e.getMessage());
            }
        }
        isFirst = false;
    }

    private void updateKnowledge(String type, String format, byte[] contents) throws IOException {
        if (configuration.get(CommonInternalConfig.INIT_CHECK) == 1) {
            if (contents == null || contents.length == 0) {
                if (isFirst) {
                    throw new RuntimeException("download knowledge database error, type is :" + type);
                } else {
                    logger.error("download knowledge database error, type:" + type + " ignored");
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
            if ((configuration.get(CommonInternalConfig.FQDN_WHOIS_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("whois_registrant_org");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                WhoisUtils.readCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.FQDN_CATEGORY_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("reputation_level");
                needColumns.add("category_name");
                needColumns.add("category_group");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                CategoryUtils.readCategoryCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.DNS_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("ip_addr");
                needColumns.add("dns_server_role");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                DnsServerUtils.readServerCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.FQDN_ICP_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("fqdn");
                needColumns.add("icp_company_name");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IcpUtils.readIcpCsv(highCsvReader);
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
                InternalIpUtils.readInternalIpCsv(highCsvReader);
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
        logger.info("update " + type + " finished, speed " + (end - start) + " ms");
    }

    private String buildExpr() {
        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.type in ['");
        sb.append(configuration.get(CommonInternalConfig.IP_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.ASN_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.FQDN_CATEGORY_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.DNS_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.FQDN_ICP_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.LINK_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.IDC_RENTER_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.APPSKT_TYPE));
        sb.append("','");
        sb.append(configuration.get(CommonInternalConfig.FQDN_WHOIS_TYPE));
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
        updateKnowledge(configuration.get(CommonInternalConfig.IP_TYPE), "mmdb", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IP_TYPE) + ".mmdb"));
        updateKnowledge(configuration.get(CommonInternalConfig.ASN_TYPE), "mmdb", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.ASN_TYPE) + ".mmdb"));
        updateKnowledge(configuration.get(CommonInternalConfig.FQDN_CATEGORY_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.FQDN_CATEGORY_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.DNS_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.DNS_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.FQDN_ICP_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.FQDN_ICP_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.LINK_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.LINK_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IDC_RENTER_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IDC_RENTER_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.APPSKT_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.APPSKT_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.FQDN_WHOIS_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.FQDN_WHOIS_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.PSIPHON_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.PSIPHON_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE) + ".csv"));
    }
}
