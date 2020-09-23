package org.apache.rocketmq.delay.util;

import org.apache.commons.lang3.StringUtils;


import java.util.Map;

public class ConstantsUtils {

    public static final String STORE_ROOT = "store.root";
    public static final String LOG_STORE_ROOT = "/data";
    public static final String LOG_RETENTION_CHECK_INTERVAL_SECONDS = "log.retention.check.interval.seconds";
    public static final int DEFAULT_LOG_RETENTION_CHECK_INTERVAL_SECONDS = 60;
    public static final String ENABLE_DELETE_EXPIRED_LOGS = "log.expired.delete.enable";

    public static final int SUCCESS = 0;
    public static final int STORE_ERROR = 6;


    public final static int MESSAGE_MAGIC_CODE = -626843481;


    public static final String SCHEDULETIMEKEY = "scheduleTime";

    public static final String DELAYKEY = "delayTime";

    public static long getScheduleTime(Map<String, String> properties){
        String propertyScheduleTime = properties.get(ConstantsUtils.SCHEDULETIMEKEY);
        if(StringUtils.isEmpty(propertyScheduleTime)){
            return 0;
        }
        try{
            return Long.parseLong(propertyScheduleTime);
        }catch (Exception e){ }
        return 0;
    }

}
