package org.apache.rocketmq.delay.store.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.delay.ScheduleIndex;
import org.apache.rocketmq.delay.config.StoreConfiguration;
import org.apache.rocketmq.delay.store.DefaultDelaySegmentValidator;
import org.apache.rocketmq.delay.store.PeriodicFlushService;
import org.apache.rocketmq.delay.store.ScheduleLogValidatorSupport;
import org.apache.rocketmq.delay.store.appender.ScheduleSetAppender;
import org.apache.rocketmq.delay.base.LongHashSet;
import org.apache.rocketmq.delay.store.model.*;
import org.apache.rocketmq.delay.store.visitor.LogVisitor;
import org.apache.rocketmq.delay.util.ConstantsUtils;
import org.apache.rocketmq.delay.util.Disposable;
import org.apache.rocketmq.delay.wheel.WheelLoadCursor;
import com.google.common.collect.Lists;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;


public class ScheduleLog implements Log<ScheduleIndex, LogRecord>, Disposable {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(ScheduleLog.class);

    private final ScheduleSet scheduleSet;
    private final AtomicBoolean open;
    private final StoreConfiguration config;
    private Object lock = new Object();
    private static final int DEFAULT_FLUSH_INTERVAL = 500;
    private final ThreadPoolExecutor slaveSyncPool ;

    public ScheduleLog(StoreConfiguration storeConfiguration) {
        final ScheduleSetSegmentContainer setContainer = new ScheduleSetSegmentContainer(
                storeConfiguration, new File(storeConfiguration.getScheduleLogStorePath())
                , new DefaultDelaySegmentValidator(), new ScheduleSetAppender(storeConfiguration.getSingleMessageLimitSize()));

        this.config = storeConfiguration;
        this.scheduleSet = new ScheduleSet(setContainer);
        this.open = new AtomicBoolean(true);
        reValidate(storeConfiguration.getSingleMessageLimitSize());

        int NCPU = Runtime.getRuntime().availableProcessors();
        int poolSize = NCPU * 2 < 1 ? 1 : NCPU * 2;
        this.slaveSyncPool = new ThreadPoolExecutor(poolSize,poolSize,0, TimeUnit.SECONDS
                ,new ArrayBlockingQueue(4096),new ThreadFactoryBuilder().setNameFormat("slave-sync-%d").build()
                ,new ThreadPoolExecutor.DiscardPolicy());
    }

    public PeriodicFlushService.FlushProvider getProvider() {
        return new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return DEFAULT_FLUSH_INTERVAL;
            }

            @Override
            public void flush() {
                ScheduleLog.this.flush();
            }
        };
    }
    private void reValidate(int singleMessageLimitSize) {
        ScheduleLogValidatorSupport support = ScheduleLogValidatorSupport.getSupport(config);
        Map<Long, Long> offsets = support.loadScheduleOffsetCheckpoint();
        scheduleSet.reValidate(offsets, singleMessageLimitSize);
    }

    @Override
    public AppendLogResult<ScheduleIndex> append(LogRecord record) {
        if (!open.get()) {
            return new AppendLogResult<>(ConstantsUtils.STORE_ERROR, "schedule log closed");
        }
        AppendLogResult<RecordResult<ScheduleSetSequence>> result = scheduleSet.append(record);
        int code = result.getCode();
        if (ConstantsUtils.SUCCESS != code) {
            LOGGER.error("append schedule set error,log:{} ,code:{}", record.getSubject(), code);
            return new AppendLogResult<>(ConstantsUtils.STORE_ERROR, "appendScheduleSetError");
        }

        RecordResult<ScheduleSetSequence> recordResult = result.getAdditional();
        ScheduleIndex index = new ScheduleIndex(
                record.getSubject(),
                record.getScheduleTime(),
                recordResult.getResult().getWroteOffset(),
                recordResult.getResult().getWroteBytes());

        return new AppendLogResult<>(ConstantsUtils.SUCCESS, "", index);
    }

    public AppendLogResult<ScheduleIndex> appendScheduleLog(MessageExt msgExt) throws Exception {
        if (!open.get()) {
            return new AppendLogResult<>(ConstantsUtils.STORE_ERROR, "schedule log closed");
        }
        long scheduleTime = 0;
        String propertyScheduleTime = msgExt.getProperty(ConstantsUtils.SCHEDULETIMEKEY);
        if(StringUtils.isEmpty(propertyScheduleTime)){
            LOGGER.error("appendScheduleLog schedule set file error,subject:{},the reason is no scheduleTime attribute", msgExt.getTopic());
            return new AppendLogResult<>(ConstantsUtils.STORE_ERROR, "no scheduleTime attribute", null);
        }else {
            try{
                scheduleTime =  Long.parseLong(propertyScheduleTime);
            }catch (Exception e){
                LOGGER.error("appendScheduleLog schedule set file error,subject:{},the reason is scheduleTime is not plastic", msgExt.getTopic());
                return new AppendLogResult<>(ConstantsUtils.STORE_ERROR, "scheduleTime is not plastic", null);
            }
        }
        MessageAccessor.clearProperty(msgExt,ConstantsUtils.SCHEDULETIMEKEY);
        MessageAccessor.clearProperty(msgExt,MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        msgExt.putUserProperty(ConstantsUtils.DELAYKEY,Long.toString(scheduleTime));
        byte[] msgBytes = MessageDecoder.encode(msgExt, false);
        ByteBuffer byteBuffer = ByteBuffer.wrap(msgBytes);
        String topic = msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
        if(StringUtils.isEmpty(topic)){
            topic = msgExt.getTopic();
        }
        LogRecord record = new ScheduleRecord(topic, scheduleTime,  msgBytes.length,  byteBuffer);
        return append(record);
    }

    public void appendSlaveScheduleLog(byte[] bodyData, Function<ScheduleIndex, Boolean> func){
        if(bodyData == null || bodyData.length == 0){
            return ;
        }
        slaveSyncPool.execute(() -> {
            try {
                ByteBuffer byteBuffer = ByteBuffer.wrap(bodyData);
                MessageExt messageExt = MessageDecoder.decode(byteBuffer);
                if(ConstantsUtils.getScheduleTime(messageExt.getProperties()) > 0){
                    AppendLogResult<ScheduleIndex> result =  appendScheduleLog(messageExt);
                    int code = result.getCode();
                    if (ConstantsUtils.SUCCESS == code) {
                        func.apply(result.getAdditional());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("slave sync error:{}",e);
            }
        });
    }
    @Override
    public boolean clean(Long key) {
        return scheduleSet.clean(key);
    }

    @Override
    public void flush() {
        synchronized(lock){
            if (open.get()) {
                scheduleSet.flush();
            }
        }
    }

    public List<ScheduleRecord> recoverLogRecord(List<ScheduleIndex> pureRecords) {
        List<ScheduleRecord> records = Lists.newArrayListWithCapacity(pureRecords.size());
        for (ScheduleIndex index : pureRecords) {
            ScheduleRecord logRecord = scheduleSet.recoverRecord(index);
            if (logRecord == null) {
                LOGGER.error("schedule log recover null record");
                continue;
            }

            records.add(logRecord);
        }

        return records;
    }

    public void clean() {
        scheduleSet.clean();
    }

    public WheelLoadCursor.Cursor loadUnDispatch(ScheduleSetSegment segment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> func) {
        LogVisitor<ScheduleIndex> visitor = segment.newVisitor(0, config.getSingleMessageLimitSize());
        try {
            long offset = 0;
            while (true) {
                Optional<ScheduleIndex> recordOptional = visitor.nextRecord();
                if (!recordOptional.isPresent()) break;
                ScheduleIndex index = recordOptional.get();
                offset = index.getOffset() + index.getSize();
                if (!dispatchedSet.contains(index.getOffset())) {
                    func.accept(index);
                }
            }
            return new WheelLoadCursor.Cursor(segment.getSegmentBaseOffset(), offset);
        } finally {
            visitor.close();
            LOGGER.info("schedule log recover {} which is need to continue to dispatch.", segment.getSegmentBaseOffset());
        }
    }

    public ScheduleSetSegment loadSegment(long segmentBaseOffset) {
        return scheduleSet.loadSegment(segmentBaseOffset);
    }

    @Override
    public void destroy() {
        open.set(false);
        ScheduleLogValidatorSupport.getSupport(config).saveScheduleOffsetCheckpoint(checkOffsets());
        slaveSyncPool.shutdownNow();
    }

    private Map<Long, Long> checkOffsets() {
        return scheduleSet.countSegments();
    }

    public long higherBaseOffset(long low) {
        return scheduleSet.higherBaseOffset(low);
    }
}
