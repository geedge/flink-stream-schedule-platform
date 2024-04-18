package com.zdjizhi.pre.relation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertEquals;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/18 17:09
 */
public class RelationTest {

    @Test
    public void test() {
        List<String> tags = new ArrayList<>();
        List<String> serverIpTagList = new ArrayList<>();
        serverIpTagList.add("tag1");
        serverIpTagList.add("tag2");
        serverIpTagList.add("tag3");
        Optional.of(serverIpTagList).ifPresent(serverIpTags -> serverIpTags.stream().map(tag -> "ip." + tag).forEach(tags::add));
        assertEquals(List.of("ip.tag1", "ip.tag2", "ip.tag3"), tags);
    }
}
