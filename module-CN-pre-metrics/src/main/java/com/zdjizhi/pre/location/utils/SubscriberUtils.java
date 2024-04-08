package com.zdjizhi.pre.location.utils;

import java.util.Random;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:29
 */
public class SubscriberUtils {

    private static Locate[] locates = new Locate[]{Locate.L1, Locate.L2, Locate.L3, Locate.L4, Locate.L5, Locate.L6, Locate.L7, Locate.L8, Locate.L9, Locate.L10, Locate.L11, Locate.L12, Locate.L13, Locate.L14, Locate.L15, Locate.L16, Locate.L17, Locate.L18, Locate.L19, Locate.L20, Locate.L21, Locate.L22, Locate.L23, Locate.L24, Locate.L25};

    public static Locate getRandomLocate() {
        Random random = new Random();
        int i = random.nextInt(25);
        return locates[i];
    }

    public enum Locate {
        L1(116.391721, 39.906094),
        L2(116.386739, 39.906266),
        L3(116.384863, 39.902834),
        L4(116.387807, 39.900607),
        L5(116.39328, 39.898911),
        L6(116.381896, 39.906917),
        L7(116.376717, 39.906147),
        L8(116.376113, 39.909896),
        L9(116.372452, 39.913832),
        L10(116.375427, 39.920883),
        L11(116.382980, 39.925003),
        L12(116.38813, 39.926376),
        L13(116.400375, 39.926834),
        L14(116.399913, 39.932007),
        L15(116.393221, 39.934137),
        L16(116.38916, 39.935303),
        L17(116.385799, 39.935684),
        L18(116.384354, 39.935303),
        L19(116.38298, 39.934273),
        L20(116.37999, 39.93446),
        L21(116.378914, 39.93676),
        L22(116.37787, 39.942691),
        L23(116.374113, 39.943599),
        L24(116.370041, 39.944283),
        L25(116.367037, 39.947116);


        public Double longitude;
        public Double latitude;

        Locate(Double longitude, Double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }
}
