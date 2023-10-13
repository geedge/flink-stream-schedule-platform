package com.zdjizhi.etl;

import com.zdjizhi.etl.utils.Trie;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TrieTest {

    @Test
    public void TrieGetTest() {
        Trie<String> trie = new Trie<>();
        trie.put(StringUtils.reverse("baidu.com"), "1");
        trie.put(StringUtils.reverse("test.baidu.com"), "2");
        trie.put(StringUtils.reverse("test1.baidu.com"), "3");

        List<String> strings1 = trie.get(StringUtils.reverse("test.baidu.com"));
        Assert.assertArrayEquals("test", Arrays.asList("1","2").toArray(), strings1.toArray());

        List<String> strings2 = trie.get(StringUtils.reverse("baidu.com"));
        Assert.assertArrayEquals("test", Arrays.asList("1").toArray(), strings2.toArray());

        List<String> strings3 = trie.get(StringUtils.reverse("test.test.baidu.com"));
        Assert.assertArrayEquals("test", Arrays.asList("1","2").toArray(), strings3.toArray());

        List<String> strings4 = trie.get(StringUtils.reverse("test1.test.baidu.com"));
        Assert.assertArrayEquals("test", Arrays.asList("1","2").toArray(), strings4.toArray());

        List<String> strings5 = trie.get(StringUtils.reverse("test1.test1.baidu.com"));
        Assert.assertArrayEquals("test", Arrays.asList("1","3").toArray(), strings5.toArray());

        List<String> strings6 = trie.get(StringUtils.reverse(""));
        Assert.assertArrayEquals("test", Arrays.asList().toArray(), strings6.toArray());

        List<String> strings7 = trie.get(StringUtils.reverse(null));
        Assert.assertArrayEquals("test", Arrays.asList().toArray(), strings7.toArray());
    }
}
