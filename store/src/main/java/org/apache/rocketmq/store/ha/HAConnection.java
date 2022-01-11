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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final HAService haService;
    // 连接的socketChannel
    private final SocketChannel socketChannel;
    // 客户端地址
    private final String clientAddr;
    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;
    // Slave请求发送的offset
    private volatile long slaveRequestOffset = -1;
    // Slave响应的offset
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        // 读的缓存内存
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 处理包的位置
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 向SocketChannel注册OP_READ事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 最多阻塞1S钟,当有一个Channel发送了数据后，立即中断阻塞
                    // 若1S时间内也没有Channel发送了数据,则processReadEvent()会立即返回true,然后再次阻塞,循环如此
                    // 等待读事件
                    this.selector.select(1000);
                    // 处理读事件
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }
                    // 若两次读事件的时间间隔超出了既定的值，则Master和slave连接失效，跳出循环
                    //  // Slave超过20S没有返回数据，断开连接 相当于心跳失效了
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
            // 断开连接 & 暂停写线程 & 暂停读线程
            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            // 重置读取缓冲区以及processPosition
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 从Channel里读取数据,可能Slave没有发送过来进度
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        // 更新readSizeZeroTimes、lastReadTimestamp
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        // 传递过来的数据大于8字节,有效的同步,8个字节代表着有一个有效的CommitLog Offset
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            // 读取Slave 请求来的CommitLog的最大位置
                            // position减去8的余数,这么做也是是为了防止流传输数据的粘包问题
                            // 获取离byteBufferRead.position()最近的8的整除数（获得当前最后一个完整的包)
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 读取ByteBuffer中position内最大的8字节的倍数,比如 position = 571,则 571-(571%8)= 571-3 = 568
                            // 也就是读取 560-568位置的字节,因为Slave可能发送了多个进度过来,Master只读最末尾也就是最大的那个
                            // 读取最后一个有效的8字节
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;
                            // 设置Slave CommitLog的最大位置
                            //更新Slaver Broker反馈的已拉取完成的数据偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            // 设置Slave 第一次请求的位置,初始化 slaveRequestOffset = Slave的请求位置
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }
                            // 通知目前Slave进度。主要用于Master节点为同步类型的。
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            //连续读取3此没有数据,break,返回true,休眠
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }
    /**
     * 写入Message线程服务
     */
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        private final int headerSize = 8 + 4;
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        // 下一次传输的位置
        private long nextTransferFromWhere = -1;
        // CommitLog读取内容
        private SelectMappedBufferResult selectMappedBufferResult;
        // 上一次写是否结束 因为通道是非阻塞的，不一定写完 单线程的
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 等待写事件
                    this.selector.select(1000);
                    // 从未获得Slave读取进度请求，sleep等待。Slave第一次请求时会将初始化 slaveRequestOffset = Slave原始进度,可能为0,也可能是宕机重启,大于0
                    // 只有Slave同步了其原始的maxPhyOffset时，Master才知道从哪里开始将数据同步给Slave
                    // 如果slaveRequestOffset等于-1，说明Master还未收到Slave Broker的拉取请求，放弃本次事件处理
                    // slaveRequesOffset在收到slave Broker拉取请求时更新
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }
                    //如果nextTransferFromWhere为-1表示初次进行数据传输，需要计算需要传输的物理偏移量
                    //如果slaveRequestOffset为8，则从当前最后一个commitLog文件起始偏移量开始传输，否则根据Slave Broker的拉取请求偏移量开始传输
                    if (-1 == this.nextTransferFromWhere) {
                        //Slave的CommitLog里没有任何数据,当前请求是其第一次请求
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            //从lastMappedFile的0位开始,也就是会忽略之前的MappedFile,只从最后一个文件开始同步
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            //如果CommitLog是中途宕机了,重启,那么会接着上次的位置继续传输
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    if (this.lastWriteOver) {

                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
                        //如果距离上次同步(写数据给Slave)超过5S,也就是5S内没有新的消息,发送一个空包过去,只有进度没有消息body,刷新Slave的最后写入时间
                        //写的间隔超过了预定心跳发送的间隔，发送一个心跳包，心跳包中没有commitlog数据
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        // 上次传输未完成，继续传输
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }
                    // 提取CommitLog指定Offset nextTransferFromWhere后的内容
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        //一次最多同步传输32KB
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size; //更新下次同步的起始位置,当前位置 + 修正过的size

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);    // 头部大小12字节,前8 Offset ; 后4 是 totalSize
                        this.byteBufferHeader.putLong(thisOffset);  // 将同步的CommitLog的Offset存入
                        this.byteBufferHeader.putInt(size);         // 将同步的数据size存入
                        this.byteBufferHeader.flip();               // 转为写模式
                        // 传输数据
                        this.lastWriteOver = this.transferData();
                    } else {
                        // 没新的消息，挂起等待
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();
            // 断开连接 & 暂停写线程 & 暂停读线程 & 释放CommitLog
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                // 心跳包在这里就结束了
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
