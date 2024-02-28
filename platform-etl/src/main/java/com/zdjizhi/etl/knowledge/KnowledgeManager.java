package com.zdjizhi.etl.knowledge;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.annotation.JSONField;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.etl.utils.ai.DomainVpnUtils;
import com.zdjizhi.etl.utils.ai.IpVpnUtils;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import com.zdjizhi.etl.utils.fqdn.CategoryUtils;
import com.zdjizhi.etl.utils.fqdn.IcpUtils;
import com.zdjizhi.etl.utils.fqdn.WhoisUtils;
import com.zdjizhi.etl.utils.ip.IPUtils;
import com.zdjizhi.etl.utils.ip.InternalIpUtils;
import com.zdjizhi.etl.utils.tag.CustomKnowledgeManager;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class KnowledgeManager {

    private final Logger logger = LoggerFactory.getLogger(KnowledgeManager.class);

    public Configuration configuration;

    private final Map<String, String> knowLedgeFileSha256 = new HashMap<>();

    private CustomKnowledge ipKnowledge = new CustomKnowledge();
    private CustomKnowledge domainKnowledge = new CustomKnowledge();
    private CustomKnowledge appKnowledge = new CustomKnowledge();

    private final List<String> knowledgeList;

    private boolean isFirst = true;

    public KnowledgeManager(Configuration configuration) {
        this.configuration = configuration;
        this.knowledgeList = getAllCategory();
    }

    public void processNacosInfo(String nacosInfo) {
        logger.info("receive nacos config:" + nacosInfo);
        List<Knowledge> knowledges = JSON.parseArray(nacosInfo, Knowledge.class);
        knowledges = knowledges.stream().filter(knowledge -> "latest".equals(knowledge.version) && knowledgeList.contains(knowledge.category)).collect(Collectors.toList());
        logger.info("resolve " + knowledges.size() + " metadata from the config info pushed by nacos");
        loadKnowledge(knowledges);
    }

    private void checkMetaList(List<Knowledge> knowledges) {
        List<String> categorys = knowledges.stream().map(knowledge -> knowledge.category).collect(Collectors.toList());
        getAllInternalCategory().forEach(category -> {
            if (!categorys.contains(category)) {
                throw new RuntimeException("can not find type :" + category + " from knowledge database");
            }
        });
    }

    private List<String> getAllInternalCategory() {
        return Arrays.asList(
                configuration.get(CommonInternalConfig.IP_TYPE),
                configuration.get(CommonInternalConfig.ASN_TYPE),
                configuration.get(CommonInternalConfig.FQDN_CATEGORY_TYPE),
                configuration.get(CommonInternalConfig.DNS_TYPE),
                configuration.get(CommonInternalConfig.FQDN_ICP_TYPE),
                configuration.get(CommonInternalConfig.LINK_TYPE),
                configuration.get(CommonInternalConfig.IDC_RENTER_TYPE),
                configuration.get(CommonInternalConfig.INTERNAL_IP_TYPE),
                configuration.get(CommonInternalConfig.APPSKT_TYPE),
                configuration.get(CommonInternalConfig.FQDN_WHOIS_TYPE),
                configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE),
                configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE),
                configuration.get(CommonInternalConfig.IP_VPN_TYPE),
                configuration.get(CommonInternalConfig.DOMAIN_VPN_TYPE)
        );
    }

    private List<String> getAllCustomCategory() {
        return Arrays.asList(
                configuration.get(CommonInternalConfig.IP_TAG_TYPE),
                configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE),
                configuration.get(CommonInternalConfig.APP_TAG_TYPE)
        );
    }

    private List<String> getAllCategory() {
        List<String> allCategory = new ArrayList<>();
        allCategory.addAll(getAllInternalCategory());
        allCategory.addAll(getAllCustomCategory());
        return allCategory;
    }

    private void loadKnowledge(List<Knowledge> knowledges) {
        List<String> tagTypes = new ArrayList<>(3);
        tagTypes.add(configuration.get(CommonInternalConfig.IP_TAG_TYPE));
        tagTypes.add(configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE));
        tagTypes.add(configuration.get(CommonInternalConfig.APP_TAG_TYPE));

        CustomKnowledge ipKnowledge = new CustomKnowledge();
        CustomKnowledge domainKnowledge = new CustomKnowledge();
        CustomKnowledge appKnowledge = new CustomKnowledge();

        try {
            if (isFirst && configuration.get(CommonInternalConfig.INIT_CHECK) == 1) {
                checkMetaList(knowledges);
            }
            Integer checkMax = configuration.get(CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER);
            knowledges.forEach(knowledge -> {
                if (tagTypes.contains(knowledge.category)) {
                    if (knowledge.isValid != 1) {
                        return;
                    }
                    if (configuration.get(CommonInternalConfig.IP_TAG_TYPE).equals(knowledge.category)) {
                        ipKnowledge.addMetadata(knowledge);
                    } else if (configuration.get(CommonInternalConfig.DOMAIN_TAG_TYPE).equals(knowledge.category)) {
                        domainKnowledge.addMetadata(knowledge);
                    } else if (configuration.get(CommonInternalConfig.APP_TAG_TYPE).equals(knowledge.category)) {
                        appKnowledge.addMetadata(knowledge);
                    }
                } else {
                    String fileName = knowledge.category + "." + knowledge.format;
                    if (!knowLedgeFileSha256.containsKey(fileName) || !knowLedgeFileSha256.get(fileName).equals(knowledge.sha256)) {
                        byte[] downloadBytes = CustomKnowledge.downloadFile(knowledge.path, knowledge.sha256, knowledge.isValid, checkMax);
                        if (downloadBytes.length == 0 && knowLedgeFileSha256.containsKey(fileName)) {
                            return;
                        }
                        updateKnowledge(knowledge.category, knowledge.format, downloadBytes);
                        knowLedgeFileSha256.put(fileName, knowledge.sha256);
                    }
                }
            });

            IPUtils.load();

            if (!this.ipKnowledge.compare(ipKnowledge)) {
                this.ipKnowledge = ipKnowledge;
                Map<String, byte[]> contents = ipKnowledge.downloadKnowledge(checkMax);
                CustomKnowledgeManager.updateIpKnowledge(contents);
            }
            if (!this.domainKnowledge.compare(domainKnowledge)) {
                this.domainKnowledge = domainKnowledge;
                Map<String, byte[]> contents = domainKnowledge.downloadKnowledge(checkMax);
                CustomKnowledgeManager.updateDomainKnowledge(contents);
            }
            if (!this.appKnowledge.compare(appKnowledge)) {
                this.appKnowledge = appKnowledge;
                Map<String, byte[]> contents = appKnowledge.downloadKnowledge(checkMax);
                CustomKnowledgeManager.updateAppKnowledge(contents);
            }
        } catch (Exception e) {
            logger.error("nacos info load error:" + e.getMessage(), e);
            if (isFirst) {
                throw new RuntimeException("knowledge init fail:" + e.getMessage());
            }
        }
        isFirst = false;
    }

    private void updateKnowledge(String type, String format, byte[] contents) {
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
                needColumns.add("app_company");
                needColumns.add("app_company_category");
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
            if ((configuration.get(CommonInternalConfig.IP_VPN_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("addr_format");
                needColumns.add("ip1");
                needColumns.add("ip2");
                needColumns.add("vpn_service_name");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                IpVpnUtils.readCsv(highCsvReader);
            }
            if ((configuration.get(CommonInternalConfig.DOMAIN_VPN_TYPE)).equals(type)) {
                List<String> needColumns = new ArrayList<>();
                needColumns.add("domain");
                needColumns.add("vpn_service_name");
                HighCsvReader highCsvReader = new HighCsvReader(reader, needColumns);
                DomainVpnUtils.readCsv(highCsvReader);
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

    @Deprecated
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
        updateKnowledge(configuration.get(CommonInternalConfig.IP_VPN_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IP_VPN_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.DOMAIN_VPN_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.DOMAIN_VPN_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE) + ".csv"));
        updateKnowledge(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE), "csv", fileUtils.getFileBytes(configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE) + ".csv"));
    }

    @Data
    public final static class Knowledge {

        @JSONField(name = "kb_id")
        private String id;

        private String name;

        private String sha256;

        private String format;

        private String path;

        private String category;

        @JSONField(name = "is_valid")
        private int isValid = 1;

        private String version;
    }

    public static void close() {
        AppUtils.close();
        WhoisUtils.close();
        CategoryUtils.close();
        DnsServerUtils.close();
        IcpUtils.close();
        LinkUtils.close();
        IdcRenterUtils.close();
        InternalIpUtils.close();
        IPUtils.close();
        IpVpnUtils.close();
        DomainVpnUtils.close();
        IocDarkWebUtils.close();
        IocMalwareUtils.close();
        CustomKnowledgeManager.close();
    }
}
