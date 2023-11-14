package com.zdjizhi.etl;

import com.zdjizhi.etl.utils.Trie;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
        Assert.assertArrayEquals("test", Arrays.asList("1", "3").toArray(), strings5.toArray());

        List<String> strings6 = trie.get(StringUtils.reverse(""));
        Assert.assertArrayEquals("test", Arrays.asList().toArray(), strings6.toArray());

        List<String> strings7 = trie.get(StringUtils.reverse(null));
        Assert.assertArrayEquals("test", Arrays.asList().toArray(), strings7.toArray());
    }

    @Test
    public void testPutAndGet() {
        Trie<String> trie = new Trie<>();

        // Test putting a single data
        trie.put("apple", "fruit");
        assertEquals(java.util.Arrays.asList("fruit"), trie.get("apple"));

        // Test putting multiple data
        trie.putAll("banana", java.util.Arrays.asList("fruit", "yellow"));
        assertEquals(java.util.Arrays.asList("fruit", "yellow"), trie.get("banana"));

        // Test putting data with the same prefix
        trie.put("app", "application");
        trie.put("app", "appendix");
        assertEquals(java.util.Arrays.asList("application", "appendix"), trie.get("app"));

        // Test putting and getting with empty string
        trie.put("", "empty");
        assertEquals(java.util.Arrays.asList(), trie.get(""));
    }

    @Test
    public void testPutAll() {
        Trie<Integer> trie = new Trie<>();

        // Test putting multiple data at once
        trie.putAll("one", java.util.Arrays.asList(1, 11, 111));
        assertEquals(java.util.Arrays.asList(1, 11, 111), trie.get("one"));

        // Test putting an empty list
        trie.putAll("empty", java.util.Arrays.asList());
        assertEquals(java.util.Arrays.asList(), trie.get("empty"));
    }

    @Test
    public void testGet() {
        Trie<Double> trie = new Trie<>();

        // Test getting data with non-existent prefix
        assertEquals(java.util.Arrays.asList(), trie.get("nonexistent"));

        // Test getting data with partially matched prefix
        trie.put("test", 3.14);
        trie.put("testing", 2.71);
        assertEquals(java.util.Arrays.asList(), trie.get("tes"));

        // Test getting data with fully matched prefix
        assertEquals(java.util.Arrays.asList(3.14), trie.get("test"));

        // Test getting data with empty string
        trie.put("empty", 0.0);
        assertEquals(java.util.Arrays.asList(), trie.get(""));
    }
}
