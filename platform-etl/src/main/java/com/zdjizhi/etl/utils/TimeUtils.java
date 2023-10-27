package com.zdjizhi.etl.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {

    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public static synchronized String getCurrentTime() {
        return FORMATTER.format(new Date());
    }

}
