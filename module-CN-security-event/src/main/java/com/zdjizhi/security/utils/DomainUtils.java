package com.zdjizhi.security.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/28
 * @ Description
 */
public class DomainUtils {
    private static final String RE_TOP_DOMAIN = "(com\\.cn|net\\.cn|gov\\.cn|org\\.nz|org\\.cn|com|net|org|gov|cc|biz|info|cn|co|me|top|best)";
    // 一级域名提取
    private static final String RE_TOP_1 = "(\\w*\\.?){1}" + RE_TOP_DOMAIN;
//            + RE_TOP_DOMAIN;

    // 二级域名提取
    private static final String RE_TOP_2 = "(\\w*\\.?){2}\\." + RE_TOP_DOMAIN;

    // 三级域名提取
    private static final String RE_TOP_3 = "(\\w*\\.?){3}\\." + RE_TOP_DOMAIN;

    //url子页面
    private static final String regex = "^((https|http|ftp|rtsp|mms)?://)"
            + "?(([0-9a-z_!~*'().&=+$%-]+: )?[0-9a-z_!~*'().&=+$%-]+@)?" //ftp的user@
            + "(([0-9]{1,3}\\.){3}[0-9]{1,3}" // IP形式的URL- 199.194.52.184
            + "|" // 允许IP和DOMAIN（域名）
            + "([0-9a-z_!~*'()-]+\\.)*" // 域名- www.
            + "([0-9a-z][0-9a-z-]{0,61})?[0-9a-z]\\." // 二级域名
            + "[a-z]{2,6})" // first level domain- .com or .museum
            + "(:[0-9]{1,4})?" // 端口- :80
            + "((/?)|" // a slash isn't required if there is no file name
            + "(/[0-9a-z_!~*'().;?:@&=+$,%#-]+)+/?)$";

    private static final Pattern PATTEN_IP = Pattern.compile("((http://)|(https://))?((\\d+\\.){3}(\\d+))");
    private static final Pattern PATTEN_TOP1 = Pattern.compile(RE_TOP_1);
    private static final Pattern PATTEN_TOP2 = Pattern.compile(RE_TOP_2);
    private static final Pattern PATTEN_TOP3 = Pattern.compile(RE_TOP_3);


    public static String getDomain(String domain, int level) {
        Matcher matcher = PATTEN_IP.matcher(domain);
        if (matcher.find()){
            return matcher.group(4);
        }

        switch (level) {
            case 1:
                matcher = PATTEN_TOP1.matcher(domain);
                break;
            case 2:
                matcher = PATTEN_TOP2.matcher(domain);
                break;
            case 3:
                matcher = PATTEN_TOP3.matcher(domain);
                break;
            default:
                return "";
        }
        if (matcher.find()) {
            return matcher.group(0);
        }
        return "";
    }

    public static String getUrl(String url) {
        Pattern p = Pattern.compile(regex);
        Matcher matcher = p.matcher(url);
        if (matcher.find()){
            return matcher.group();
        }

        return "";
    }

    public static void main(String[] args) {
//        String[] urls = {
//                "googleapis.best",
//                "testin.cn",
//                "portmap.pro.testin.cn:8888/portmap",
//        };
//
//        for (String url : urls) {
////            System.out.println("一级域名："+getDomain(url,1));
//            System.out.println("二级域名："+getDomain(url,3));
////            System.out.println("三级域名："+getDomain(url, 3));
//        }


        String cn_record="http://appwebstat.biz/info.php";
        String ioc="http://appwebstat.biz/info.php";
        String s = cn_record + "/";
        if (s.contains(ioc)){
            System.out.println(s);
        }

//        long l = System.currentTimeMillis();//1646118723
//        long l = 1646118723;
//        System.out.println(l*1000);
//        Date date = new Date(l*1000);
//        Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(date);
//        long time = granularityFiveMinute.getTime();
//        System.out.println(time);
//        System.out.println(granularityFiveMinute);
    }
}
