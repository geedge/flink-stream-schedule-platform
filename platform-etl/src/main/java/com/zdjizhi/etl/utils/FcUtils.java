package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.etl.common.DomainCategoryReputation;
import com.zdjizhi.etl.common.DomainIcp;
import com.zdjizhi.utils.StringUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class FcUtils {
    private static Logger LOG = LoggerFactory.getLogger(FcUtils.class);

    private static HashMap<String, DomainCategoryReputation> smap = readCsv(CommonConfig.WEBSKT_PATH);
    private static HashMap<String, DomainIcp> icpmap = readIcpCsv(CommonConfig.ICP_PATH);

    private static HashMap<String, DomainCategoryReputation> readCsv(String path) {
        HashMap<String , DomainCategoryReputation> fcmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            for (String[] strs : csvReader) {
                try {
                    String key = strs[0];
                    if (!key.startsWith("#")){
                        DomainCategoryReputation catalog = new DomainCategoryReputation();
                        catalog.setFqdn(key);
                        catalog.setMatch_pattern(strs[1]);
                        catalog.setCategoryName(strs[6]);
                        catalog.setCategoryGroup(strs[7]);
                        catalog.setReputationLevel(strs[4]);
                        String score= strs[3];
                        if (StringUtil.isNotBlank(score)){
                            catalog.setReputationScore(Long.parseLong(score));
                        }
                        fcmap.put(key, catalog);
                    }
                }catch (Exception e){
                    LOG.error("加载{}失败 \n{}",strs,e);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error("加载失败", e);
        }
        return  fcmap;
    }

    private static HashMap<String, DomainIcp> readIcpCsv(String path) {
        HashMap<String , DomainIcp> icpmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[0];
                if (!key.startsWith("#")){
                    DomainIcp catalog = new DomainIcp();
                    catalog.setFqdn(key);
                    catalog.setIcpCompanyName(strs[5]);
                    icpmap.put(key, catalog);

                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return  icpmap;
    }

    public static void setWebSketchMessage(CnRecordLog entity) {
        String domain = entity.getDomain();
        if(smap.containsKey(domain)){
            DomainCategoryReputation domainCategoryReputation = smap.get(domain);
            entity.setDomain_reputation_score(domainCategoryReputation.getReputationScore());
            entity.setDomain_category_group(domainCategoryReputation.getCategoryGroup());
            entity.setDomain_category_name(domainCategoryReputation.getCategoryName());
            entity.setDomain_reputation_level(domainCategoryReputation.getReputationLevel());
        }
    }

    public static void setIcpMessage(CnRecordLog entity) {
        String domain = entity.getDomain();
        if(icpmap.containsKey(domain)){
            entity.setDomain_icp_company_name(icpmap.get(domain).getIcpCompanyName());
        }
    }


    public static void main(String[] args) throws Exception {
        HashMap<String, DomainCategoryReputation> map = readCsv(CommonConfig.WEBSKT_PATH);
        Set<String> keySet = map.keySet();

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(CommonConfig.ES_HOST);
        RestClientBuilder lowLevelRestClient = RestClient.builder(esAddresses.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(lowLevelRestClient);

        BulkRequest bulkRequest = new BulkRequest();
        int i = 0;
        for (String key : keySet) {
            DomainCategoryReputation categoryReputation = map.get(key);
            String matchPattern = categoryReputation.getMatch_pattern();
            if ("1".equals(matchPattern)){
                try {
                    String categoryName = categoryReputation.getCategoryName();
                    String categoryGroup = categoryReputation.getCategoryGroup();
                    long reputationScore = categoryReputation.getReputationScore();
                    String reputationLevel = categoryReputation.getReputationLevel();

                    i+=1;

                    XContentBuilder builder = XContentFactory.jsonBuilder()
                            .startObject()
                            .field("id", key)
                            .field("entity_type", "domain")
                            .field("found_time", System.currentTimeMillis() / 1000)
                            .field("update_time", System.currentTimeMillis() / 1000)
                            .field("domain_name", key)
                            .field("domain_category", categoryName)
                            .field("domain_category_group", categoryGroup)
                            .field("domain_reputation_score", reputationScore)
                            .field("domain_reputation_level", reputationLevel)
                            .endObject();
                    UpdateRequest source = new UpdateRequest("entity_info",key)
                            .doc(builder)
                            .upsert(builder)
                            .id(key);
                    bulkRequest.add(source);

                    if (i>=com.zdjizhi.etl.common.CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN){
                        client.bulk(bulkRequest,RequestOptions.DEFAULT);
                        LOG.info("写入{}",i);
                        i=0;
                    }
                } catch (Exception e) {
                    LOG.error("写入{}失败 \n{}", categoryReputation,e);
                }
            }
        }
        client.bulk(bulkRequest,RequestOptions.DEFAULT);
        LOG.info("写入完毕");
        client.close();
        System.out.println(map.size());
    }
}