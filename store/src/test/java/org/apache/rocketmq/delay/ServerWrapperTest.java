package org.apache.rocketmq.delay;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.delay.startup.ServerWrapper;
import org.apache.rocketmq.delay.util.ConstantsUtils;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

public class ServerWrapperTest {

    /**
     * t
     * defaultMessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
     */
    String testMessageDelayLevel = "5s 8s";
    /**
     * choose delay level
     */
    int delayLevel = 2;

    private static final String storePath = System.getProperty("user.home") + File.separator + "schedule_test#" + UUID.randomUUID();
    private static final int commitLogFileSize = 1024;
    private static final int cqFileSize = 10;
    private static final int cqExtFileSize = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    static String sendMessage = " ------- schedule message test -------";
    static String topic = "schedule_topic_test";
    static String messageGroup = "delayGroupTest";

    ServerWrapper wrapper;
    DefaultMessageStore messageStore;
    MessageStoreConfig messageStoreConfig;
    BrokerConfig brokerConfig;

    private static SocketAddress bornHost;
    private static SocketAddress storeHost;


    static {
        try {
            bornHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            storeHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() throws Exception {
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMessageDelayLevel(testMessageDelayLevel);
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(true);
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        brokerConfig = new BrokerConfig();
        BrokerStatsManager manager = new BrokerStatsManager(brokerConfig.getBrokerClusterName());
        messageStore = new DefaultMessageStore(messageStoreConfig, manager, new MyMessageArrivingListener(), new BrokerConfig());

        assertThat(messageStore.load()).isTrue();

        messageStore.start();
        wrapper = new ServerWrapper(messageStore,null);
    }

    @Test
    public void testAppend() throws Exception {
        MessageExtBrokerInner msg = buildMessage();
        wrapper.getFacade().appendScheduleLog(msg);
        Thread.sleep(20000);
    }

    @Test
    public void testPutMessage() throws Exception {
        MessageExtBrokerInner msg = buildMessage();
        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.isOk()).isTrue();
        Thread.sleep(20000);
    }

    private MessageExtBrokerInner buildMessage() {

        byte[] msgBody = sendMessage.getBytes();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("schedule_tag");
        msg.setKeys("schedule_key");
        msg.setBody(msgBody);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setDelayTimeLevel(delayLevel);
        msg.putUserProperty(ConstantsUtils.SCHEDULETIMEKEY,String.valueOf(getDateTime(Calendar.SECOND,5)));
        return msg;
    }

    private long getDateTime(int field, int amount){
        Calendar calendar = Calendar.getInstance();
        calendar.add(field, amount);
        return calendar.getTime().getTime();
    }
    @After
    public void close(){
        messageStore.destroy();
        wrapper.destroy();
    }
    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
