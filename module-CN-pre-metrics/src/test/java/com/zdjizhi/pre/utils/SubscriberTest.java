package com.zdjizhi.pre.utils;


import com.zdjizhi.pre.location.utils.SubscriberUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/10/29 14:19
 */
public class SubscriberTest {

    @Test
    public void test() {
        String url = "http://www.baidu.com?lat=123&lon=456";
        SubscriberUtils.Locate locate = SubscriberUtils.getLocateByURL(url);
        Assert.assertEquals(locate.getLatitude(), 123.0, 0.0);
        Assert.assertEquals(locate.getLongitude(), 456.0, 0.0);
    }

    @Test
    public void test2() {
        String url = "http://www.baidu.com?latitude=123&longitude=456";
        SubscriberUtils.Locate locate = SubscriberUtils.getLocateByURL(url);
        Assert.assertEquals(locate.getLatitude(), 123.0, 0.0);
        Assert.assertEquals(locate.getLongitude(), 456.0, 0.0);
    }
}
