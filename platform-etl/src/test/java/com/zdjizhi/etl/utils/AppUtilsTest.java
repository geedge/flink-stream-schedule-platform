package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.utils.JsonMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class AppUtilsTest {

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    @Before
    public void setUp() throws Exception {
        String path = this.getClass().getResource("/app_sketch_labeled.json").getPath();
        System.out.println("path:" + path);
//        AppUtils.readJson(path);
    }

    @Test
    public void setAppSketchMessage() {
        CnRecordLog cnRecordLog = new CnRecordLog();

        String json = "{\"common_app_id\":{\"THIRD\":[{\"app_id\":3974}]}}";
        cnRecordLog = jsonMapperInstance.fromJson(json,CnRecordLog.class);

        //cnRecordLog.setCommon_app_id("{\"THIRD\":[{\"app_id\":3520,\"surrogate_id\":0}]},{\"THIRD\":[{\"app_id\":3520,\"surrogate_id\":0}]}");
        cnRecordLog.setCommon_app_label("douyin");

        AppUtils.setAppSketchMessage(cnRecordLog);

        assertEquals("douyin",cnRecordLog.getCommon_app_label());
        assertEquals("multimedia-streaming",cnRecordLog.getApp_subcategory());
        assertEquals(1,cnRecordLog.getApp_risk());
        assertEquals("media",cnRecordLog.getApp_category());
        assertEquals(Integer.valueOf(0),cnRecordLog.getApp_is_protocol());
        assertEquals("Bytedance",cnRecordLog.getApp_company());
    }
}