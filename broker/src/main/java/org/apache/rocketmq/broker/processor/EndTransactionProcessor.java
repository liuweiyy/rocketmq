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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * EndTransaction processor: process commit and rollback message
 */
public class EndTransactionProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * commit消息=>从commitLog读取出prepare消息=>检查prepare消息=>转换为真正待消费消息=>追加到commitLog文件=>删除prepare消息=>ReputMessageService把待消费消息转储到consumeQueue=>客户端消费事务消息。
     * rollback消息=>从commitLog读取出prepare消息=>检查prepare消息=>删除prepare消息。
     * 该方法的核心就是根据EndTransactionRequestHeader上送的commitLogPhysOffset找到prepare消息，然后还原消息保存到commitLog内，也很容易理解。
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
        RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        /*
         *	该requestHeader包含了
         *	producerGroup==producerGroupName，
         *	tranStateTableOffset==prepare消息在consumeQueue的位置（表示该消息在cq上是第几条消息）， （用于把原始的事务消息保存到consumeQueue上，即保存在prepare消息在consumeQueue的位置）
         *	commitLogOffset==prepare消息在commitLog的绝对位置，（用于查找在commitLog上的commitLogOffset位置的prepare消息，把prepare消息转换为原始消息，继而最后保存到commitLog上）
         *	commitOrRollback==事务消息类型TRANSACTION_COMMIT_TYPE/TRANSACTION_ROLLBACK_TYPE/TRANSACTION_NOT_TYPE，
         *	transactionId==消息的 UNIQ_KEY
         *	msgId==消息的UNIQ_KEY
         *	fromTransactionCheck是否是broker回查事务，true是，false否
         */
        final EndTransactionRequestHeader requestHeader =
            (EndTransactionRequestHeader)request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.debug("Transaction request:{}", requestHeader);
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }
        //1.Producer执行完本地事务后立即发来END_TRANSACTION请求
        //2.Broker发送回查后，Producer检查本地事务状态后，发来END_TRANSACTION请求
        //这里二者逻辑的区别也仅仅是日志记录不同。
        if (requestHeader.getFromTransactionCheck()) {
            //表示是否是回查检查消息。用于broker发producer消息回查事务，producer结束事务发送到broker的时候，该值为true。对于producer发送prepare消息后执行完本地事务，发送commit/rollback消息到broker的时候，该值为false。
            //回查事务和非回查，执行功能是一样的
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());

                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {//对应事务状态的UNKNOW，不处理
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {//事务commit消息，处理
                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {//事务rollback消息，处理
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }
        OperationResult result = new OperationResult();
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            //commitMessage提交消恳。真实的逻辑是根据传过来的commitLogOffset从CommitLog获取half消息
            //事务commit消息，则直接将原先发的prepare从commitLog文件读出来消息转换为原消息，并写入commitLog，消息的topic是原topic，即被消费者订阅可以消费到
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);//根据EndTransactionRequestHeader.commitLogOffset这个commitlog物理偏移量从commitlog中查找到prepare消息
            if (result.getResponseCode() == ResponseCode.SUCCESS) {//从commitlog中查找到了prepare消息
                //检查合法性: i.half消息的PGROUP、queueOffset、commitLogOffset和请求头的相应值相同。
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());// 还原half message真实的topic、queueId
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));// sysFlag没置为TRANSACTION_COMMIT_TYPE
                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());//设置原始消息在consumequeue的offset，即保存到prepare消息在consumequeue上的位置。
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());//prepare消息在commitlog的绝对（物理）位置，即commitlog格式中的PTO
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());//原始消息的存储时间戳为prepare消息存储时间戳
                    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    // 将还原后的消息重新落盘，随后就会调用CommitLogDispatcherBuildConsumeQueue.dispatch更新原始Topic的ConsumerQueue
                    // 该消息对消费者可见。
                    RemotingCommand sendResult = sendFinalMessage(msgInner);//把原始消息写入到commitlog
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {//原始消息写入commitlog成功，从commitlog删除prepare消息
                        // 删除half消息(并非从磁盘真正删除)。
                        // 具体逻辑是构建一个消息放入RMQ_SYS_TRANS_OP_HALF_TOPIC队列，queueId为对应half消息queueId
                        // 消息内容为对应half消息的queueOffset，并将TAGS属性设置"d"。
                        // 所谓删除prepare消息就是把该消息写入到commitlog，topic是op half topic，这样broker回查的时候判断OP HALF有了该消息，就不再进行回查
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }
        } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
            //rollbackMessage,真实逻辑和commitMessage一样。真实的逻辑是根据传过来的commitLogOffset从CommitLog获取half消息
            //如果是Rollback，则直接将消息转换为原消息，并写入到Op Topic里
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    // 删除half消息，具体逻辑是将消息放入RMQ_SYS_TRANS_OP_HALF_TOPIC队列，消息内容为对应half消息的queueOffset，并将TAGS属性设置"d"。
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;//设置返回结果，实际producer是oneway发送方式，不返回producer
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }

            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }

            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
            (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
