package com.zdjizhi.base.rule;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/28 16:07
 */
public class ClickhousePasswordTest {

    public static void main(String[] args) {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("galaxy");
        System.out.println(encryptor.encrypt("default"));
        System.out.println(encryptor.decrypt("LXDp+zqdQqDIIqaDfqsKoA=="));
        System.out.println(encryptor.encrypt("galaxy2019"));
        System.out.println(encryptor.decrypt("RY+0nruXpPqITsQ3ob4P7Qbd8W246+Pa"));
    }
}
