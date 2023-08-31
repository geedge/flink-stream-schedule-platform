package com.zdjizhi.etl.utils;

import com.google.common.net.InternetDomainName;
import org.apache.commons.lang3.StringUtils;

public class DomainUtils {

    private static final String HTTP_URL_PREFIX = "http://";
    private static final String SSL_URL_PREFIX = "https://";
    private static final String FTP_URL_PREFIX = "ftp://";


    public static String getTopPrivateDomain(String domain) {
        String result = "";
        domain = getDomain(domain);
        try {
            InternetDomainName internetDomainName = InternetDomainName.from(domain);
            result = internetDomainName.isUnderPublicSuffix() ? internetDomainName.topPrivateDomain().toString() : "";
        } catch (IllegalArgumentException ignored) {
        }
        return result;
    }

    public static String getDomain(String url) {
        if (StringUtils.isBlank(url)) {
            return "";
        } else {
            if (url.indexOf(HTTP_URL_PREFIX, 0) >= 0) {
                url = url.substring(HTTP_URL_PREFIX.length(), url.length());
            } else if (url.indexOf(SSL_URL_PREFIX, 0) >= 0) {
                url = url.substring(SSL_URL_PREFIX.length(), url.length());
            } else if (url.indexOf(FTP_URL_PREFIX, 0) >= 0) {
                url = url.substring(FTP_URL_PREFIX.length(), url.length());
            } else {
                url = url;
            }

            if (url.indexOf("www.") == 0) {
                url = url.substring(url.indexOf(".") + 1, url.length());
            }

            if (url.indexOf(":") >= 0) {
                url = url.substring(0, url.indexOf(":"));
            }

            if (url.indexOf("/", 0) >= 0) {
                url = url.substring(0, url.indexOf("/", 1));
            }

            if (url.indexOf("?") >= 0) {
                url = url.substring(0, url.indexOf("?"));
            }

            return url;
        }
    }


}
