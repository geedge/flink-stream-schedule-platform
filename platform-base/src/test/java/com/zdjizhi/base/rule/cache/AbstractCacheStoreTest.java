package com.zdjizhi.base.rule.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractCacheStoreTest {

    @Mock
    private CacheStore.UpdateListener<String, Integer> mockListener;

    private AbstractCacheStore<String, Integer> cacheStore;

    @BeforeEach
    void setUp() {
        cacheStore = new AbstractCacheStore<String, Integer>() {};
        cacheStore.addListener(mockListener);
    }

    @RepeatedTest(8)
    void testConcurrent() {
        Random random = new Random();
        int value1 = random.nextInt(1000);
        int value2 = random.nextInt(1000);
        cacheStore.put("value1", value1);
        cacheStore.put("value2", value2);

        verify(mockListener, times(1)).onPut("value1", value1);
        verify(mockListener, times(1)).onPut("value2", value2);

        Optional<Integer> get1 = cacheStore.get("value1");
        Optional<Integer> get2 = cacheStore.get("value2");

        assertTrue(get2.isPresent());
        assertTrue(get1.isPresent());
        verify(mockListener, times(1)).onGet("value1", get1.get());
        verify(mockListener, times(1)).onGet("value2", get2.get());

        cacheStore.clear();
        verify(mockListener, times(1)).onClear();
    }

    @Test
    void testGetAndPut() {
        cacheStore.put("value1", 1);
        cacheStore.put("value2", 2);

        verify(mockListener, times(1)).onPut("value1", 1);
        verify(mockListener, times(1)).onPut("value2", 2);

        Optional<Integer> value1 = cacheStore.get("value1");
        assertTrue(value1.isPresent());
        assertEquals(value1.get(), 1);
        verify(mockListener, times(1)).onGet("value1", 1);

        Optional<Integer> value3 = cacheStore.get("value3");
        assertFalse(value3.isPresent());
        assertEquals(value3, Optional.empty());
        verify(mockListener, times(1)).onGet("value3", null);
    }

    @Test
    void testPutAll() {
        Map<String, Integer> map = IntStream.range(0, 100)
                .boxed()
                .collect(Collectors.toMap(i -> "value" + i, i -> i));

        cacheStore.putAll(map);
        verify(mockListener, times(100)).onPut(any(), any());
    }
}