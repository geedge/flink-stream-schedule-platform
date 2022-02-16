package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class dnsUtils {
    private static Logger LOG = LoggerFactory.getLogger(dnsUtils.class);
    private static HashMap<String, CnRecordLog> dnsmap = readServerCsv(CommonConfig.DNS_PATH);

    private static HashMap<String, CnRecordLog> readServerCsv(String path) {
        HashMap<String , CnRecordLog> fcmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[1];
                ArrayList<String> serverRole=new ArrayList<>();
                if (!key.startsWith("#")){
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
        return  fcmap;
    }

//    private static HashMap<String, CnRecordLog> readServerCsv1(String path) {
//        HashMap<String , CnRecordLog> flmap =new HashMap<>();
//        try {
//            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
//            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
//                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
//
//            for (String[] strs : csvReader) {
//                String key = strs[1];
//                if (!key.startsWith("#")){
//                    CnRecordLog catalog = new CnRecordLog();
//                    catalog.setDoh_support(Integer.parseInt(strs[12]));
//                    catalog.setDot_support(Integer.parseInt(strs[11]));
//                    if (entity.getCommon_l7_protocol().equals("DNS")){
//                        if (entity.getCommon_server_port()==443 && dnsmap.get(richEntity.getCommon_server_ip()).getDoh_support()==1){
//                            richEntity.setDns_protocol("doh");
//                            System.out.println(entity.getCommon_server_port());
//                            System.out.println(richEntity.getDns_protocol());
//                            flmap.put(key, catalog);
//                        }else {
//                            richEntity.setDns_protocol("dns");
//                            System.out.println(entity.getCommon_server_port());
//                            System.out.println(richEntity.getDns_protocol());
//                            flmap.put(key, catalog);
//                        }
//                        if (entity.getCommon_server_port()==853 && dnsmap.get(richEntity.getCommon_server_ip()).getDot_support()==1){
//                            richEntity.setDns_protocol("dot");
//                            System.out.println(entity.getCommon_server_port());
//                            System.out.println(richEntity.getDns_protocol());
//                            flmap.put(key, catalog);
//                        }else {
//                            richEntity.setDns_protocol("dns");
//                            System.out.println(entity.getCommon_server_port());
//                            System.out.println(richEntity.getDns_protocol());
//                            flmap.put(key, catalog);
//                        }
//                        if (entity.getDns_sub()==1){
//                            richEntity.setDns_protocol("dnssec");
//                            System.out.println(richEntity.getDns_protocol());
//                            flmap.put(key, catalog);
//                        }else {
//                            richEntity.setDns_protocol("dns");
//                            System.out.println(richEntity.getDns_protocol());
//                            flmap.put(key, catalog);
//                        }
////                if ((entity.getCommon_server_port()!=443 && dnsmap.get(richEntity.getCommon_server_ip()).getDoh_support()!=1) && (entity.getCommon_server_port()!=853 && dnsmap.get(richEntity.getCommon_server_ip()).getDot_support()!=1) && entity.getDns_sub()!=1){
////                    richEntity.setDns_protocol("dns");
////                }
//                    }else {
//                        richEntity.setDns_protocol("");
//                        flmap.put(key, catalog);
//                    }
////                    protocolmap.put(key, catalog);
//                }
//            }
//            csvReader.close();
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//        }
//        return  flmap;
//    }



    public static void getWebSktMessage(CnRecordLog cnRecordLog){
        try{
            if (dnsmap.containsKey(cnRecordLog.getCommon_server_ip())){
                cnRecordLog.setDns_server_role(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_role());
                cnRecordLog.setDns_server_org(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_org());
                cnRecordLog.setDns_server_os(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_os());
                cnRecordLog.setDns_server_software(dnsmap.get(cnRecordLog.getCommon_server_ip()).getDns_server_software());
            }
        }catch (Exception e) {
            LOG.error(e.toString());
        }
    }


    public static void main(String[] args) {
        HashMap<String, CnRecordLog> map = readServerCsv(CommonConfig.DNS_PATH);
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
    }

