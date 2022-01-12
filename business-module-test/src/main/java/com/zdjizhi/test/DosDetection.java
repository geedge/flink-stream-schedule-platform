package com.zdjizhi.test;

import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.etl.DosSketchEtl;

public class DosDetection implements Schedule {
    @Override
    public void schedule() throws Exception {
        DosSketchEtl.getSketchSource().print();
    }
}
