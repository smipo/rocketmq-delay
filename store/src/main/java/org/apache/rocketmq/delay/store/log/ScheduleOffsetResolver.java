package org.apache.rocketmq.delay.store.log;

import org.joda.time.LocalDateTime;

public class ScheduleOffsetResolver {

    static {
        LocalDateTime.now();
    }

    public static long resolveSegment(long offset, int scale) {
        LocalDateTime localDateTime = new LocalDateTime(offset);
        long year = year(localDateTime);
        long month = month(localDateTime);
        long day = day(localDateTime);
        long hour = hour(localDateTime);
        long minute = minute(localDateTime);
        minute = minute - (minute % scale);
        return year + month + day + hour + minute;
    }

    private static long year(final LocalDateTime localDateTime) {
        return localDateTime.getYear() * 100000000L;
    }

    private static long month(final LocalDateTime localDateTime) {
        return localDateTime.getMonthOfYear() * 1000000L;
    }

    private static long day(final LocalDateTime localDateTime) {
        return localDateTime.getDayOfMonth() * 10000L;
    }

    private static long hour(final LocalDateTime localDateTime) {
        return localDateTime.getHourOfDay() * 100L;
    }

    private static long minute(final LocalDateTime localDateTime) {
        return localDateTime.getMinuteOfHour();
    }
}
