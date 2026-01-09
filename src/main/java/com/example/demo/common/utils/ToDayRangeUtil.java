package com.example.demo.common.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ToDayRangeUtil {

    // 获取今天早上7点到明天早上6点的日期范围 ，如果当前时间在今天早上7点之前，返回昨天的7点到今天的6点
    public static String getToDayRange() {
        // 获取今天早上7点的时间
        LocalDateTime today7am = LocalDateTime.now().withHour(7).withMinute(0).withSecond(0).withNano(0);

        // 获取明天早上6点的时间
        LocalDateTime tomorrow6am = today7am.plusDays(1).withHour(6);

        // 获取当前时间
        LocalDateTime currentTime = LocalDateTime.now();

        // 计算日期范围
        String startDate = getFormattedDate(today7am);
        String endDate = getFormattedDate(tomorrow6am);

        // 如果当前时间在今天早上7点之前
        if (currentTime.isBefore(today7am)) {
            // 当前时间在今天7点之前，则输出昨天早上7点到今天早上6点
            startDate = getFormattedDate(today7am.minusDays(1));
            endDate = getFormattedDate(today7am.minusHours(1)); // 今天早上6点
        } else if (currentTime.isBefore(tomorrow6am)) {
            // 当前时间在今天早上7点到明天早上6点之间
            // 输出今天早上7点到明天早上6点
            startDate = getFormattedDate(today7am);
            endDate = getFormattedDate(tomorrow6am); // 明天早上6点
        }

        return startDate + "-" + endDate;
    }

    // 获取当前时间对应的 yyyy-MM-dd 日期
    public static String getCurrentRangeDate() {
        LocalDateTime today7am = LocalDateTime.now().withHour(7).withMinute(0).withSecond(0).withNano(0);
        LocalDateTime currentTime = LocalDateTime.now();

        if (currentTime.isBefore(today7am)) {
            // 如果当前时间在早上 7 点之前，属于前一天
            return today7am.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else {
            // 否则属于今天
            return today7am.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }
    }

    // 格式化日期为 yyyyMMdd07 或 yyyyMMdd06
    private static String getFormattedDate(LocalDateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDate = dateTime.format(formatter);
        // 根据小时判断是07点还是06点
        return formattedDate + (dateTime.getHour() == 7 ? "07" : "06");
    }

    // 获取昨天的今天早上7点
    public static LocalDateTime getYesterday7am() {
        LocalDateTime today7am = LocalDateTime.now().withHour(7).withMinute(0).withSecond(0).withNano(0);
        return today7am.minusDays(1);
    }

    // 获取明天的今天早上6点
    public static LocalDateTime getTomorrow6am() {
        LocalDateTime today7am = LocalDateTime.now().withHour(7).withMinute(0).withSecond(0).withNano(0);
        return today7am.plusDays(1).withHour(6);
    }

    public static void main(String[] args) {
        // 示例调用
        System.out.println("日期范围: " + ToDayRangeUtil.getToDayRange());
        System.out.println(getCurrentRangeDate());
    }

}
