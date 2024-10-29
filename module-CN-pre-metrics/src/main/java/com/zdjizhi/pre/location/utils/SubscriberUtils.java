package com.zdjizhi.pre.location.utils;

import java.util.Random;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/27 16:29
 */
public class SubscriberUtils {

    private static LocateEnum[] locateEnums = new LocateEnum[]{LocateEnum.L1, LocateEnum.L2, LocateEnum.L3, LocateEnum.L4, LocateEnum.L5,
            LocateEnum.L6, LocateEnum.L7, LocateEnum.L8, LocateEnum.L9, LocateEnum.L10,
            LocateEnum.L11, LocateEnum.L12, LocateEnum.L13, LocateEnum.L14, LocateEnum.L15,
            LocateEnum.L16, LocateEnum.L17, LocateEnum.L18, LocateEnum.L19, LocateEnum.L20,
            LocateEnum.L21, LocateEnum.L22, LocateEnum.L23, LocateEnum.L24, LocateEnum.L25};

    private static final Random random = new Random();

    public static Locate getLocateByURL(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }

        //pre
        if (url.contains("?")) {
            url = url.split("\\?")[1];
        }
        // first rule
        if (url.contains("lat=") && url.contains("lon=")) {
            String[] split = url.split("&");
            for (String s : split) {
                if (s.contains("lat=")) {
                    String[] split1 = s.split("=");
                    if (split1.length == 2) {
                        try {
                            Double latitude = Double.valueOf(split1[1]);
                            for (String s1 : split) {
                                if (s1.contains("lon=")) {
                                    String[] split2 = s1.split("=");
                                    if (split2.length == 2) {
                                        Double longitude = Double.valueOf(split2[1]);
                                        return new Locate(longitude, latitude);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
            }
        }

        //second rule
        if (url.contains("latitude") && url.contains("longitude")) {
            String[] split = url.split("&");
            for (String s : split) {
                if (s.contains("latitude")) {
                    String[] split1 = s.split("=");
                    if (split1.length == 2) {
                        try {
                            Double latitude = Double.valueOf(split1[1]);
                            for (String s1 : split) {
                                if (s1.contains("longitude")) {
                                    String[] split2 = s1.split("=");
                                    if (split2.length == 2) {
                                        Double longitude = Double.valueOf(split2[1]);
                                        return new Locate(longitude, latitude);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
            }
        }

        return null;
    }

    public static Locate getRandomLocate() {
        int i = random.nextInt(25);
        return transformLocate(locateEnums[i]);
    }

    private static Locate transformLocate(LocateEnum locateEnum) {
        return new Locate(locateEnum.getLongitude(), locateEnum.getLatitude());
    }

    public static class Locate {
        private Double longitude;
        private Double latitude;

        Locate(Double longitude, Double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public Double getLongitude() {
            return longitude;
        }

        public Double getLatitude() {
            return latitude;
        }
    }

    public enum LocateEnum {
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


        private Double longitude;
        private Double latitude;

        LocateEnum(Double longitude, Double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public Double getLongitude() {
            return longitude;
        }

        public Double getLatitude() {
            return latitude;
        }
    }
}
