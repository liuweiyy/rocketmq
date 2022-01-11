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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
/**
 * 高可用服务
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();
    // master use
    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;
    // WriteSocketService等待CommmitLog追加
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    // 所有Slave中的最大同步偏移量
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;
    // slave use
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }
    /**
     * BrokerController#initialize() 时,如果发现当前角色是Slave,则将haMasterAddress的值更新到haClient中
     */
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }
    /**
     * 推送到Slave的Offset是否小于这条消息的Offset
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }
    /**
     * 通知slave进度
     *
     * @param offset slave进度
     */
    public void notifyTransferSome(final long offset) {
        //若slaveAckOffset大于HAService.push2SlaveMaxOffset的值
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // 更新push2SlaveMaxOffset的值
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                //通知调用GroupTransferService.notifyTransferSome法唤醒GroupTransferService服务线程
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                // 若值已经改变则继续循环
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;
        /**
         * port ：监听Slave请求的端口, 10912
         */
        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 监听是否有socket连接
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            // 有新的socket连接
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
                                // 接收到Slave的请求,生成一个HAConnection,用户向Channel写数据以及读取Slave传过来的数据
                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 创建HAConnection
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

        public void putRequest(final CommitLog.GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        /**
         * 通知slave进度。唤醒等待锁。
         */
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doWaitTransfer() {
            if (!this.requestsRead.isEmpty()) {
                for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                    // 推送到Slave的Offset是否 >= 当前消息的Offset
                    boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                    //每次从Slave的进度提交请求能够中断wait
                    //最多等5次Slave的Ack,如果Ack的进度 >= 当前消息的进度,则返回true
                    while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                        this.notifyTransferObject.waitForRunning(1000);// 唤醒
                        transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    }

                    if (!transferOK) {
                        log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                    }
                    // 唤醒请求，并设置是否Slave同步成功
                    req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                }

                this.requestsRead = new LinkedList<>();
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    class HAClient extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**
         * Master节点地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        //汇报slave最大偏移量的reportOffset的byteBuffer
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        // java NIO 多路复用
        private Selector selector;
        /**
         * 最后读取数据时间，即Master写入socket事件
         * 记录；两次写的时间间隔
         */
        private long lastWriteTimestamp = System.currentTimeMillis();
        // Slave CommitLog当前汇报的最大偏移量
        private long currentReportedOffset = 0;
        /**
         * {@link #byteBufferRead}处理的到位
         * 主要处理粘包问题，保证数据读取全
         * 指向了当前byteBufferRead中首个未被解析的commitLog数据包
         */
        private int dispatchPosition = 0;
        /**
         * 读取数据字节缓冲区,缓冲区大小默认4MB
         * 接收commitLog数据包的bytebuffer
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * 读取数据字节缓冲区备份
         * byteBufferRead的备份
         * 当{@link #byteBufferRead}写入已满时，未处理完内容放到该变量{@link #reallocateByteBuffer()}
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }
        /**
         * 更新Master地址
         *
         * @param newAddr Master地址
         */
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 是否满足上报进度的时间
         * 距离上一次上报进度时间超过5S , 也就是每5S上报一次进度
         *
         * @return 是否
         */
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }
        /**
         * 上报进度,传递进度的时候仅传递一个Long类型的Offset,8个字节,没有其他数据
         *
         * @param maxOffset 进度
         * @return 是否上报成功
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            // clear方法
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            // flip方法
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    // 发送SlaveMaxOffset
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            // 若超过3次还未能将8字节发送到Master Broker ,说明存在问题，返回false
            return !this.reportOffset.hasRemaining();
        }
        /**
         * 重新分配字节缓冲区
         */
        private void reallocateByteBuffer() {
            // 有剩余内容未处理，放入备份区
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            // 重置处理位置
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }
        /**
         * 处理从Master读取到数据的事件
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) { // byteBufferRead 还没有读满
                try {
                    //将从Master处通过SocketChannel获取到的数据写入 byteBufferRead,返回写入的字节数量
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 分发请求
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        // 连续3次读取均为0，则跳出循环,放弃本次读取
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }
        /**
         * 读取Master传输的CommitLog数据，并返回是否异常
         * 如果读取到数据，写入CommitLog
         * 异常原因：
         * 1. Master传输来的数据offset 不等于 Slave的CommitLog数据最大offset
         * 2. 上报到Master进度失败
         *
         * @return 是否异常
         */
        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size

            while (true) {
                // 读取到请求
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                // 看是否长度大于头部长度
                if (diff >= msgHeaderSize) {
                    // 读取masterPhyOffset、bodySize。使用dispatchPostion的原因是：处理数据“粘包”导致数据读取不完整。
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);
                    // 校验 Master传输来的数据offset 是否和 Slave的CommitLog数据最大offset 是否相同。
                    // 获取当前本地的CommitLog的偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        //数据中断了,Slave的最大Offset匹配不上推送过来的Offset
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }
                    // 读取到消息
                    // 看是否大于整个包的长度
                    if (diff >= (msgHeaderSize + bodySize)) {
                        // 写入CommitLog
                        byte[] bodyData = byteBufferRead.array();
                        int dataStart = this.dispatchPosition + msgHeaderSize;
                        // 放到自己本地的CommitLog上去
                        HAService.this.defaultMessageStore.appendToCommitLog(
                                masterPhyOffset, bodyData, dataStart, bodySize);
                        // 更新dispatchPosition
                        this.dispatchPosition += msgHeaderSize + bodySize;
                        // 上报到Master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }
                // 空间写满，重新分配空间
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        /**
         * 上报Master进度。
         * 如果上报失败，关闭连接
         *
         * @return 是否上报成功
         */
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }
        /**
         * 连接Master节点
         *
         * @return 是否连接成功
         * @throws ClosedChannelException 当注册读事件失败时
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                //当自身角色是Slave时,会将配置中的MessageStoreConfig中的haMasterAdress赋值给masterAddress,或者在registerBroker时的返回值赋值
                if (addr != null) {
                    // 获取master地址
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        // 连接master broker
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 注册读事件
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                //更新最近上报Offset,也就是当前CommitLog的maxOffset
                // currentReportedOffset为当前CommitLog的最大物理偏移
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                //更新最近写入时间
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }
        /**
         * 关闭Master节点
         */
        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 当存在masterAddress != null && 连接Master成功
                    // 连接master
                    if (this.connectMaster()) {
                        // 若距离上次上报时间超过5S，上报到Master进度
                        // 判断是否需要汇报心跳
                        if (this.isTimeToReportOffset()) {
                            // 汇报心跳，也就是SlaveMaxOffset
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }
                        // 最多阻塞1S,直到Master有数据同步于过来。若1S满了还是没有接受到数据,中断阻塞,
                        // 执行processReadEvent(),但结果读入byteBufferRead的大小为0,然后循环到这步
                        // I/O复用，检查是否有读事件
                        this.selector.select(1000);
                        // 处理读取事件 Master返回数据
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }
                        // 若进度有变化，上报到Master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        // Master超过20S未返回数据，关闭连接
                        // 超出一定事件间隔未发送Master信息，断开连接
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
