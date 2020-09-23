package org.apache.rocketmq.delay.wheel;


import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.delay.DelayLogFacade;
import org.apache.rocketmq.delay.ScheduleIndex;
import org.apache.rocketmq.delay.Switchable;
import org.apache.rocketmq.delay.config.DefaultStoreConfiguration;
import org.apache.rocketmq.delay.config.StoreConfiguration;
import org.apache.rocketmq.delay.base.LongHashSet;
import org.apache.rocketmq.delay.store.log.ScheduleSetSegment;
import org.apache.rocketmq.delay.store.model.DispatchRecord;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.log.DispatchLogSegment;
import org.apache.rocketmq.delay.store.log.ScheduleOffsetResolver;
import org.apache.rocketmq.delay.store.visitor.LogVisitor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WheelTickManager implements Switchable, HashedWheelTimer.Processor {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(WheelTickManager.class);

    private static final int TICKS_PER_WHEEL = 2 * 60 * 60;

    private final int segmentScale;
    private final ScheduledExecutorService loadScheduler;
    private final ExecutorService loadingExecutorService;
    private final Object loadingLock = new Object();
    private final StoreConfiguration config;
    private final DelayLogFacade facade;
    private final HashedWheelTimer timer;
    private final AtomicBoolean started;
    private final WheelLoadCursor loadingCursor;
    private final WheelLoadCursor loadedCursor;

    private MessageStore writeMessageStore;

    public WheelTickManager(DefaultStoreConfiguration config, DelayLogFacade facade, MessageStore writeMessageStore) {
        this.config = config;
        this.segmentScale = config.getSegmentScale();
        this.timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("delay-send-%d").build(), 500, TimeUnit.MILLISECONDS, TICKS_PER_WHEEL, this);
        this.facade = facade;
        this.started = new AtomicBoolean(false);
        this.loadingCursor = WheelLoadCursor.create();
        this.loadedCursor = WheelLoadCursor.create();

        this.loadScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("wheel-segment-loader-%d").build());
        this.loadingExecutorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("wheel-segment-loading-%d").build());
        this.writeMessageStore = writeMessageStore;
    }

    @Override
    public void start() {
        if (!isStarted()) {
            timer.start();
            started.set(true);
            recover();
            loadScheduler.scheduleWithFixedDelay(this::load, 0, config.getLoadSegmentDelayMinutes(), TimeUnit.MINUTES);
            LOGGER.info("wheel started.");
        }
    }

    private void recover() {
        LOGGER.info("wheel recover...");
        DispatchLogSegment currentDispatchedSegment = facade.latestDispatchSegment();
        if (currentDispatchedSegment == null) {
            LOGGER.warn("load latest dispatch segment null");
            return;
        }

        long latestOffset = currentDispatchedSegment.getSegmentBaseOffset();
        DispatchLogSegment lastSegment = facade.lowerDispatchSegment(latestOffset);
        if (null != lastSegment) doRecover(lastSegment);

        doRecover(currentDispatchedSegment);
        LOGGER.info("wheel recover done. currentOffset:{}", latestOffset);
    }

    private void doRecover(DispatchLogSegment dispatchLogSegment) {
        long segmentBaseOffset = dispatchLogSegment.getSegmentBaseOffset();
        ScheduleSetSegment setSegment = facade.loadScheduleLogSegment(segmentBaseOffset);
        if (setSegment == null) {
            LOGGER.error("load schedule index error,dispatch segment:{}", segmentBaseOffset);
            return;
        }

        LongHashSet dispatchedSet = loadDispatchLog(dispatchLogSegment);
        WheelLoadCursor.Cursor loadCursor = facade.loadUnDispatch(setSegment, dispatchedSet, this::refresh);
        long baseOffset = loadCursor.getBaseOffset();
        loadingCursor.shiftCursor(baseOffset, loadCursor.getOffset());
        loadedCursor.shiftCursor(baseOffset);
    }

    private LongHashSet loadDispatchLog(final DispatchLogSegment currentDispatchLog) {
        LogVisitor<Long> visitor = currentDispatchLog.newVisitor(0);
        final LongHashSet recordSet = new LongHashSet(currentDispatchLog.entries());
        try {
            while (true) {
                Optional<Long> recordOptional = visitor.nextRecord();
                if (!recordOptional.isPresent()) break;
                recordSet.set(recordOptional.get());
            }
            return recordSet;
        } finally {
            visitor.close();
        }
    }

    private boolean isStarted() {
        return started.get();
    }

    private synchronized void load() {
        long next = System.currentTimeMillis() + config.getLoadInAdvanceTimesInMillis();
        long prepareLoadBaseOffset = ScheduleOffsetResolver.resolveSegment(next, segmentScale);
        try {
            loadUntil(prepareLoadBaseOffset);
        } catch (InterruptedException ignored) {
            LOGGER.debug("load segment interrupted");
        }
    }

    private void loadUntil(long until) throws InterruptedException {
        long loadedBaseOffset = loadedCursor.baseOffset();
        // have loaded
        if (loadedBaseOffset > until) return;

        do {
            // wait next turn when loaded error.
            if (!loadUntilInternal(until)) break;

            // load successfully(no error happened) and current wheel loading cursor < until
            if (loadingCursor.baseOffset() < until) {
                long thresholdTime = System.currentTimeMillis() + config.getLoadBlockingExitTimesInMillis();
                // exit in a few minutes in advance
                if (ScheduleOffsetResolver.resolveSegment(thresholdTime, segmentScale) >= until) {
                    loadingCursor.shiftCursor(until);
                    loadedCursor.shiftCursor(until);
                    break;
                }
            }

            Thread.sleep(100);
        } while (loadedCursor.baseOffset() < until);

        LOGGER.info("wheel load until {} <= {}", loadedCursor.baseOffset(), until);
    }

    private boolean loadUntilInternal(long until) {
        long index = resolveStartIndex();
        if (index < 0) return true;
        try {
            while (index <= until) {
                ScheduleSetSegment segment = facade.loadScheduleLogSegment(index);
                if (segment == null) {
                    long nextIndex = facade.higherScheduleBaseOffset(index);
                    if (nextIndex < 0) return true;
                    index = nextIndex;
                    continue;
                }
                loadSegment(segment);
                long nextIndex = facade.higherScheduleBaseOffset(index);
                if (nextIndex < 0) return true;
                index = nextIndex;
            }
        } catch (Throwable e) {
            LOGGER.error("wheel load segment failed,currentSegmentOffset:{} until:{}", loadedCursor.baseOffset(), until, e);
            return false;
        }

        return true;
    }

    /**
     * resolve wheel-load start index
     *
     * @return generally, result > 0, however the result might be -1. -1 mean that no higher key.
     */
    private long resolveStartIndex() {
        WheelLoadCursor.Cursor loadedEntry = loadedCursor.cursor();
        long startIndex = loadedEntry.getBaseOffset();
        long offset = loadedEntry.getOffset();

        if (offset < 0) return facade.higherScheduleBaseOffset(startIndex);

        return startIndex;
    }

    private void loadSegment(ScheduleSetSegment segment) {
        try {
            long baseOffset = segment.getSegmentBaseOffset();
            long offset = segment.getWrotePosition();
            if (!loadingCursor.shiftCursor(baseOffset, offset)) {
                LOGGER.error("doLoadSegment error,shift loadingCursor failed,from {}-{} to {}-{}", loadingCursor.baseOffset(), loadingCursor.offset(), baseOffset, offset);
                return;
            }

            WheelLoadCursor.Cursor loadedCursorEntry = loadedCursor.cursor();
            // have loaded
            if (baseOffset < loadedCursorEntry.getBaseOffset()) return;

            long startOffset = 0;
            // last load action happened error
            if (baseOffset == loadedCursorEntry.getBaseOffset() && loadedCursorEntry.getOffset() > -1)
                startOffset = loadedCursorEntry.getOffset();

            LogVisitor<ScheduleIndex> visitor = segment.newVisitor(startOffset, config.getSingleMessageLimitSize());
            try {
                loadedCursor.shiftCursor(baseOffset, startOffset);

                long currentOffset = startOffset;
                while (currentOffset < offset) {
                    Optional<ScheduleIndex> recordOptional = visitor.nextRecord();
                    if (!recordOptional.isPresent()) break;
                    ScheduleIndex index = recordOptional.get();
                    currentOffset = index.getOffset() + index.getSize();
                    refresh(index);
                    loadedCursor.shiftOffset(currentOffset);
                }
                loadedCursor.shiftCursor(baseOffset);
                LOGGER.info("loaded segment:{} {}", loadedCursor.baseOffset(), currentOffset);
            } finally {
                visitor.close();
            }
        }finally {

        }
    }

    private void refresh(ScheduleIndex index) {
        long now = System.currentTimeMillis();
        long scheduleTime = now;
        try {
            scheduleTime = index.getScheduleTime();
            timer.newTimeout(index, scheduleTime - now, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            LOGGER.error("wheel refresh error, scheduleTime:{}, delay:{}", scheduleTime, scheduleTime - now);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void shutdown() {
        if (isStarted()) {
            loadScheduler.shutdown();
            loadingExecutorService.shutdown();
            timer.stop();
            started.set(false);
            LOGGER.info("wheel shutdown.");
        }
    }

    public void addWHeel(ScheduleIndex index) {
        loadingExecutorService.execute(() -> {
            synchronized (loadingLock){
                long baseOffset = ScheduleOffsetResolver.resolveSegment(index.getScheduleTime(), segmentScale);
                loadingCursor.shiftCursor(baseOffset, index.getOffset());
                loadedCursor.shiftCursor(baseOffset,index.getOffset());
                load();
            }
        });
    }

    public boolean canAdd(long scheduleTime, long offset) {
        WheelLoadCursor.Cursor currentCursor = loadingCursor.cursor();
        long currentBaseOffset = currentCursor.getBaseOffset();
        long currentOffset = currentCursor.getOffset();

        long baseOffset = ScheduleOffsetResolver.resolveSegment(scheduleTime, segmentScale);
        if (baseOffset < currentBaseOffset) return true;

        if (baseOffset == currentBaseOffset) {
            return currentOffset <= offset;
        }
        return false;
    }

    @Override
    public void process(ScheduleIndex index) {
        if(index.getBody() == null || index.getBody().length == 0){
            return;
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(index.getBody());
        MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        PutMessageResult putMessageResult = writeMessageStore.putMessage(messageTimeup(messageExt));
        if (putMessageResult != null
                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            LogRecord record = new DispatchRecord(index.getSubject(), index.getScheduleTime(),index.getOffset());
            facade.appendDispatchLog(record);
        }else {
            // XXX: warn and notify me
            LOGGER.error(
                    "WheelTickManager, a message time up, but reput it failed, topic: {} msgId {}",
                    messageExt.getTopic(), messageExt.getMsgId());
            timer.newTimeout(index, 3, TimeUnit.SECONDS);
        }

    }

    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        if(StringUtils.isNotBlank(queueIdStr)){
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);
        }
        return msgInner;
    }
}
