/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 传入参数：transactionTimeout==60s 事务回查超时时间， transactionCheckMax==15，最大回查次数，listener是DefaultTransactionalMessageCheckListener
     * 功能：读取当前half的half queueOffset，然后从op half拉取32条消息保存到removeMap，如果half queueOffset处的消息在removeMap中，
     * 则说明该prepare消息被处理过了，然后读取下一条prepare消息，如果prepare不在removeMap中，说明是需要回查的，此时broker作为client端，向服务端producer发送回查命令，
     * 最后由producer返回回查结果更新原prepare消息。
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            // 获取RMQ_SYS_TRANS_HALF_TOPIC的所有MessageQueue(一个MessageQueue对应一个ConsumeQueue)
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);//返回的是half topic的消息队列，只有一个队列
            if (msgQueues == null || msgQueues.size() == 0) {
                //说明broker还没有接收过prepare消息，自然half topic是null
                    log.warn("The queue of topic is empty :" + topic);
                    return;
                }
                log.debug("Check topic={}, queues={}", topic, msgQueues);
                //为每一个consumeQueue执行检查逻辑，检查逻辑找到需要进行回查的half消息，并发起回查请求.
                for (MessageQueue messageQueue : msgQueues) {//遍历half topic下的消息队列，实际只有一个消息队列
                    long startTime = System.currentTimeMillis();
                    // 根据HALF_TOPIC的MessageQueue，获取OP_HALF_TOPIC对应的MessageQueue。根据queueId对应。
                    // (实际上HALF_TOPIC和OP_HALF_TOPIC各只有一个MessageQueue)
                    // HALF_TOPIC和OP_HALF_TOPIC里面的逻辑队列是一一对应的
                    MessageQueue opQueue = getOpQueue(messageQueue);//获取op half topic的消息队列(只有一个队列)，OP就是英文operator缩写
                    // 获取两个MessageQueue的当前消费进度
                    long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);//获取prepare消息的当前消费queueoffset
                    long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);//获取op half消息的当前消费queueoffset
                    log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                    if (halfOffset < 0 || opOffset < 0) {
                        log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                                halfOffset, opOffset);
                        continue;
                    }

                    List<Long> doneOpOffset = new ArrayList<>();
                    HashMap<Long, Long> removeMap = new HashMap<>();
                    // 填充上面两个集合 根据halfQueue，opQueue判断出opQueue中哪些消息已经处理过，哪些没处理过 处理过的opQueue offSet放入doneOpOffset中
                    // fillOpRemoveMap方法返回的removeMap集合包含的是已经被commit/rollback的prepare消息的queueOffset集合
                    // fillOpRemoveNap中对每一个拉取到的OP消息:比对OP消息内容（对应的half消息的逻辑队列偏移量)和half队列当前偏移量。
                    //  1，如果小于half队列当前偏移量，说明该OP消息对应的half消息已经回查过。将OP消息的逻辑队列偏移量放入doneOpOffset.
                    // 这个doneOffset只做后面推进OP逻辑队列位点使用。
                    // 2，如果大于或等于half队列当前偏移量，则将【halfOffset<--->opOffset】存入removeMap中，
                    // 当前的half消息需不需要回在就看它在不在这个RemoveMap中
                    PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                    //当前逻辑队列中没有新写入的OP消息，进行下一个队列检查
                    if (null == pullResult) {
                        log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                                messageQueue, halfOffset, opOffset);
                        continue;
                    }
                    // single thread
                    int getMessageNullCount = 1;//获取空消息的次数
                    long newOffset = halfOffset;//当前处理RMQ_SYS_TRANS_HALF_TOPIC#queueId的最新进度。
                    long i = halfOffset;//当前处理RMQ_SYS_TRANS_HALF_TOPIC消息的队列偏移量
                    while (true) {
                        // 遍历，看看queueOffset=i处的prepare消息是否在removeMap集合内，如果在，说明该prepare消息被commit/rollback处理过了，如果不在，则说明该prepare消息未被处理过，需要进行回查
                        // 对RMQ_SYS_TRANS_HALF_TOPIC的每一个MessageQueue，执行检查逻辑的时间不能超过60s
                        if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                            //这是RocketMQ处理任务的一个通用处理逻辑，就是一个任务处理，可以限制每次最多处理的时间，RocketMQ为待检测主题RMQ_SYS_TRANS_HALF_TOPIC的每个队列，做事务状态回查，一次最多不超过60S，目前该值不可配置
                            log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                            break;
                        }
                        // 如果removeMap中包含当前messageQueue的当前offset，说明该offset对应的half消息已经被committed或rollback,
                        // 不需要回查了,继续一下个offset检查(i++）
                        if (removeMap.containsKey(i)) {
                            //说明i位置处的这个prepare消息已经被commit/rollback处理过了，因此i+1，接着执行下一次while
                            log.debug("Half offset {} has been committed/rolled back", i);
                            Long removedOpOffset = removeMap.remove(i);
                            doneOpOffset.add(removedOpOffset);
                        } else {
                            // 说明i位置处的这个prepare消息还未被commit/rollback处理过，需要进行回查
                            // 获取当前MessageQueue当前offset对应的half消息
                            GetResult getResult = getHalfMsg(messageQueue, i);//从queueoffset位置获取commitlog上的prepare消息，这里的参数i表示queueoffset
                            MessageExt msgExt = getResult.getMsg();//获取queueoffset=i处的prepare消息
                            if (msgExt == null) {//prepare消息不存在
                                /*
                                 * 	如果消息为空，则根据允许重复次数进行操作，默认重试一次，目前不可配置。其具体实现为：
                                 *  1、如果超过重试次数，直接跳出，结束该消息队列的事务状态回查。
                                 *  2、如果是由于没有新的消息而返回为空（拉取状态为：PullStatus.NO_NEW_MSG），则结束该消息队列的事务状态回查。
                                 *  3、其他原因，则将偏移量i设置为： getResult.getPullResult().getNextBeginOffset()，重新拉取。
                                 */
                                if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {//空消息次数+1
                                    break;
                                }
                                if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {//prepare消息不存在，则退出
                                    log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                            messageQueue, getMessageNullCount, getResult.getPullResult());
                                    break;
                                } else {
                                    //继续从commitLog读取下一个prepare消息
                                    log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                            i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                    i = getResult.getPullResult().getNextBeginOffset();
                                    newOffset = i;
                                    continue;
                                }
                            }
                            // 如果回查次数超过设定的最大值（默认15次）或者当前half消息的落盘时间超过设定的commitLog文件保存时间（72/小时)
                            // 则放弃该half消息回查。在该队列的检查逻辑结束后更新其offset，今后也不再回查。
                            /*
                             * needDiscard，prepare消息已经被回查达到15次，则不再回查该prepare消息
                             * needSkip prepare消息存储时间距离现在超过了72h，则不再回查该prepare消息
                             * 	判断该消息是否需要discard(吞没，丢弃，不处理)、或skip(跳过)，其依据如下
                             * 	1、needDiscard 依据：如果该消息回查的次数超过允许的最大回查次数，则该消息将被丢弃，即事务消息提交失败，不能被消费者消费，其做法，主要是每回查一次，在消息属性TRANSACTION_CHECK_TIMES中增1，默认最大回查次数为15次。
                             *	2、needSkip依据：如果事务消息超过文件的过期时间，默认72小时（具体请查看RocketMQ过期文件相关内容），则跳过该消息。
                             */
                            if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                                listener.resolveDiscardMsg(msgExt);
                                newOffset = i + 1;
                                i++;
                                continue;
                            }
                            // 在当前MessageQueue的检查逻辑开始后，如果有half消息写入，会发生这种情况。
                            // 因为ConsumeQueue是按照顺序构建的，所以该MessageQueue后面的消息也都不用回查了。
                            if (msgExt.getStoreTimestamp() >= startTime) {
                                log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                        new Date(msgExt.getStoreTimestamp()));
                                break;
                            }
                            /*
                             * 	处理事务超时相关概念，先解释几个局部变量：
                             *  valueOfCurrentMinusBorn ：该消息已生成的时间,等于系统当前时间减去消息生成的时间戳。
                             *  checkImmunityTime ：立即检测事务消息的时间，其设计的意义是，应用程序在发送事务消息后，事务不会马上提交，该时间就是假设事务消息发送成功后，应用程序事务提交的时间，在这段时间内，RocketMQ任务事务未提交，故不应该在这个时间段向应用程序发送回查请求。
                             *  transactionTimeout：事务消息的超时时间，这个时间是从OP拉取的消息的最后一条消息的存储时间与check方法开始的时间，如果时间差超过了transactionTimeout，就算时间小于checkImmunityTime时间，也发送事务回查指令。
                             */
                            long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                            long checkImmunityTime = transactionTimeout;
                            String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                            // 如果生产者侧对特定消息设置了首次回查免疫时问
                            if (null != checkImmunityTimeStr) {
                                checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                                if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                    // 既然还没有到回查免疫时间，为什么还要进行一个if判断?
                                    // 对于非首次回查的消息，免疫时间不起作用。
                                    if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                        newOffset = i + 1;
                                        i++;
                                        continue;
                                    }
                                }
                            } else {
                                // 如果生产者侧没有设置首次回查免疫时间:如果在免疫时间内，则跳过该half消息以及后续所有half消息回查(因为同一ConsumeQueue中消息是顺序的）
                                if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                    log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                            checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                    break;
                                }
                            }
                            List<MessageExt> opMsg = pullResult.getMsgFoundList();
                            boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                                    || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                    || (valueOfCurrentMinusBorn <= -1);

                            if (isNeedCheck) {
                                // 如果需要发送事务状态回查消息，则先将消息再次发送到HALF_TOPIC主题中，发送成功则返回true，否则返回false
                                // 如果发送成功，会将该消息的queueOffset、commitLogOffset设置为重新存入的偏移量
                                if (!putBackHalfMsgQueue(msgExt, i)) {
                                    continue;
                                }
                                // 异步向producer发送CHECK_TRANSACTION_STATE命令查询producer本地事务状态
                                // 此时broker作为client端，producer作为服务端
                                // 具体调度在这
                                listener.resolveHalfMsg(msgExt);
                            } else {
                                // 如果无法判断是否发送回查消息，则加载更多的op(已处理)消息进行筛选
                                pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                                log.debug("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                        messageQueue, pullResult);
                                continue;
                            }
                        }
                        newOffset = i + 1;
                        i++;
                    }
                    // 更新halfTopic和opHalfTopic的消费位点
                    /*
                     *	保存(Prepare)消息队列的回查进度。保存到ConsumerOffsetManager.offsetTable，key是RMQ_SYS_TRACE_TOPIC@CID_RMQ_SYS_TRANS，
                     * 	跟普通消息的topic@groupname不同，half和op half消息消息没有使用真实的groupName，而是重新定义了系统groupName==CID_RMQ_SYS_TRANS
                     */
                    if (newOffset != halfOffset) {
                        transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                    }
                    //计算op逻辑队列需要往前推进的位置
                    long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                    if (newOpOffset != opOffset) {
                        // 保存处理队列（op）的进度。保存到ConsumerOffsetManager.offsetTable，
                        // key是RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS
                        transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                    }
                }

        } catch(Throwable e){
                log.error("Check error", e);
            }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap 处理过的prepare消息保存到该集合，key:halfQueueOffset, value: opQueueOffset
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp queue上当前queueOffset.
     * @param miniOffset 消息队列上当前queueOffset。不要被英文注释给蒙蔽了，不是最小offset，而是当前half上的queueOffset
     * @param doneOpOffset  已经被处理过的op half消息的queueuOffset保存到该集合
     * @return Op message result.
     *     具体实现逻辑是从op half主题消息队列中拉取32条，如果拉取的消息队列偏移量大于等于half topic消息队列的当前queueOffset时，会添加到removeMap中，表示已处理过。
     *     removeMap里存放prepare消息队列中已经commit或者rollback的偏移量和待操作队列的消息偏移量(发送commit或rollback后，会往待操作队列中写)
     *     doneOpOffset存放待操作队列的消息偏移量
     *     核心就是拿opQueue中消息体内的halfQueue的offSet(已经处理过的半消息offSet)与当前halfQueue的offSet做比较
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
        MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        // 从OP_HALF_TOPIC获取消息，最多32条
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            // offset非法or没有匹配的msg，说明需要更新op half的offset啦
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            // 该pullOffsetOfOp位置后没有消息，说明不需要更新op half的offset
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {
            /*
             * 	 对于op half队列内保存的消息来说
             *	 消息的body是prepare消息在consumeQueue上的queueOffset
             *	 消息的tag是TransactionalMessageUtil.REMOVETAG
             *	 在TransactionalMessageBridge.addRemoveTagInTransactionOp(MessageExt, MessageQueue)做的
             *   queueOffset变量就是prepare消息在consumeQueue上的offset
             *
             *   获取OP消息对应的half消息的queueOffset(还记得op消息的消息体存放的是其对应的half消息的queueOffset吗)
             */
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                if (queueOffset < miniOffset) {
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
