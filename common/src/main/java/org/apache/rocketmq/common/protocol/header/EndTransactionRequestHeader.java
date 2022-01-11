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

package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class EndTransactionRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String producerGroup;//发送broker前赋值	producerGroupname
    @CFNotNull
    private Long tranStateTableOffset;//发送broker前赋值	prepare消息在consumequeue的位置（表示该消息在cq上是第几条消息）
    @CFNotNull
    private Long commitLogOffset;//发送broker前赋值  prepare消息在commitlog的绝对位置
    @CFNotNull
    private Integer commitOrRollback; // TRANSACTION_COMMIT_TYPE 发送broker前赋值 为对应的消息类型commit/rollback/unknow
    // TRANSACTION_ROLLBACK_TYPE
    // TRANSACTION_NOT_TYPE

    @CFNullable
    private Boolean fromTransactionCheck = false;

    @CFNotNull
    private String msgId;//发送broker前赋值消息属性的UNIQ_KEY

    private String transactionId;//发送broker前赋值 事务id，通常是消息属性的UNIQ_KEY

    @Override
    public void checkFields() throws RemotingCommandException {
        if (MessageSysFlag.TRANSACTION_NOT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
            return;
        }

        throw new RemotingCommandException("commitOrRollback field wrong");
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public void setTranStateTableOffset(Long tranStateTableOffset) {
        this.tranStateTableOffset = tranStateTableOffset;
    }

    public Long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(Long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public Integer getCommitOrRollback() {
        return commitOrRollback;
    }

    public void setCommitOrRollback(Integer commitOrRollback) {
        this.commitOrRollback = commitOrRollback;
    }

    public Boolean getFromTransactionCheck() {
        return fromTransactionCheck;
    }

    public void setFromTransactionCheck(Boolean fromTransactionCheck) {
        this.fromTransactionCheck = fromTransactionCheck;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return "EndTransactionRequestHeader{" +
            "producerGroup='" + producerGroup + '\'' +
            ", tranStateTableOffset=" + tranStateTableOffset +
            ", commitLogOffset=" + commitLogOffset +
            ", commitOrRollback=" + commitOrRollback +
            ", fromTransactionCheck=" + fromTransactionCheck +
            ", msgId='" + msgId + '\'' +
            ", transactionId='" + transactionId + '\'' +
            '}';
    }
}
