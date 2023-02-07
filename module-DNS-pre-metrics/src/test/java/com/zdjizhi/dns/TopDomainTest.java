package com.zdjizhi.dns;

import com.google.common.net.InternetDomainName;
import com.zdjizhi.utils.FormatUtils;
import com.zdjizhi.utils.StringUtil;

public class TopDomainTest {

    public static void main(String[] args) {

        String domain = "05bobapp.s3.amazonaws.com";

        System.out.println("qname:\t"+domain);

        System.out.println("qname_sld:\t"+FormatUtils.getTopPrivateDomain(domain));

        InternetDomainName internetDomainName = InternetDomainName.from(domain);
        if (internetDomainName.isUnderPublicSuffix()) {
            System.out.println("qname_tld:\t"+internetDomainName.publicSuffix().toString());
        }


    }

}
