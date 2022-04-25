package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class IdcRenterUtils {

    private static Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static TreeRangeMap<IPAddress, String> treeRangeMap = readServerCsv(CommonConfig.IDC_RENTER_PATH);

    private static TreeRangeMap<IPAddress, String> readServerCsv(String path) {
        TreeRangeMap<IPAddress, String> rangeMap = TreeRangeMap.create();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String ip = strs[1];
                IPAddressString ipAddressString = new IPAddressString(ip);
                if (ipAddressString.isIPAddress()){
                    IPAddress address = ipAddressString.getAddress();
                    IPAddress lower = address.getLower();
                    IPAddress upper = address.getUpper();
                    rangeMap.put(Range.closed(lower, upper), strs[3]);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return rangeMap;
    }

    public static void setIdcRenter(Entity entity, CnRecordLog cnRecordLog){
        IPAddress serverAddress = new IPAddressString(entity.getCommon_server_ip()).getAddress();
        String serverRenter = treeRangeMap.get(serverAddress);
        cnRecordLog.setServer_idc_renter(serverRenter);

        IPAddress clientAddress = new IPAddressString(entity.getCommon_client_ip()).getAddress();
        String clientRenter = treeRangeMap.get(clientAddress);
        cnRecordLog.setServer_idc_renter(clientRenter);
    }

    public static void main(String[] args) {
        Map<Range<IPAddress>, String> rangeStringMap = treeRangeMap.asMapOfRanges();
        for (Range<IPAddress> range:rangeStringMap.keySet()){
            String s = rangeStringMap.get(range);
            System.out.println(range + "---" + s);
        }

        System.out.println("-----------------------");
        IPAddress address = new IPAddressString("116.178.77.200").getAddress();
        System.out.println(treeRangeMap.get(address));

    }


}
