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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 每个IndexFile的大小为420 000 040（40 + 500w * 4 + 2000w * 20)字节,400.5432510376MB
 * 文件名称为:20180227110750161 时间的格式化形式,精确到毫秒 2018-02-27:11:07:50.161
 * 文件路径为${storePath}/index/20180227110750161
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 每个 hash  槽所占的字节数
    private static int hashSlotSize = 4;
    // 每条indexFile条目占用字节数
    // 索引所占字节大小
    private static int indexSize = 20;
    // 用来验证是否是一个有效的索引。
    private static int invalidIndex = 0;
    // index 文件中 hash 槽的总个数
    // 5百万 构建索引占用的槽位数
    private final int hashSlotNum;
    // indexFile中包含的条目数
    // 2千万 构建的索引个数
    private final int indexNum;
    // 对应的映射文件
    private final MappedFile mappedFile;
    // 对应的文件通道
    private final FileChannel fileChannel;
    // 对应 PageCache
    private final MappedByteBuffer mappedByteBuffer;
    // IndexHeader,每一个indexfile的头部信息
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 40+5000000*4+5000000*4*20 = 420 000 040  =400.5432510376MB
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     *
     * @param key
     * @param phyOffset 消息存储在commitlog的偏移量
     * @param storeTimestamp 消息存入commitlog的时间戳
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 如果目前 index file 存储的条目数小于允许的条目数，则存入当前文件中，如果超出，则返回 false, 表示存入失败，
        // IndexService 中有重试机制，默认重试3次
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            // 先获取 key 的 hashcode，然后用 hashcode 和 hashSlotNum 取模，得到该 key 所在的 hashslot 下标，hashSlotNum默认500万个。
            int slotPos = keyHash % this.hashSlotNum;
            // 根据 key 所算出来的 hashslot 的下标计算出绝对位置，从这里可以看出端倪：
            // IndexFile的文件布局：文件头(IndexFileHeader 40个字节) + (hashSlotNum * 4)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {
                // 读取 key 所在 hashslot 下标处的值(4个字节)，如果小于0或超过当前包含的 indexCount，则设置为0。
                // 在插槽位置的值,代表着相同key的最新索引位置
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 时间差,消息存储时间 - 索引文件启用时间
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }// 计算消息的存储时间与当前 IndexFile 存放的最小时间差额(单位为秒）

                // 索引位置 = 40 + 5000000*4 + 已存储索引个数*20, 索引存储时有序
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                /**
                 * keyHash：消息中指定的业务key的hash值
                 * phyOffset：当前key对应的消息在commitlog中的偏移量commitlog
                 * offsetTimeDiff：当前key对应消息的存储时间与当前indexFile创建时间的时间差
                 * preIndexNo：当前slot下当前index索引单元的前一个index索引单元的indexNo
                 */
                // 根据topic-Keys或者topic-UniqueKey计算哈希值
                // 填充 IndexFile 条目，4字节（hashcode） + 8字节（commitlog offset） + 4字节（commitlog存储时间与indexfile第一个条目的时间差，单位秒） + 4字节（同hashcode的上一个的位置，0表示没有上一个）。
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);// 4byte
                // message在commitLog的物理位置
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);//4byte
                // 落地的时间 - 当前索引文件的起始时间
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);//8byte
                // 在索引数据域要把刚刚有冲突的哈希桶的位置记录下来，这样就构建成了一个LinkList
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);//4byte,若不为空,值是上一个相同key的索引顺序位置,这样查询时能够递归的按照此值获取指定key的所有索引信息
                // 更新哈希桶的索引位置，如果有冲突，刚刚已经记录下来了
                // 将当前先添加的条目的位置，存入到 key hashcode 对应的 hash槽，也就是该字段里面存放的是该 hashcode 最新的条目
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount()); //存储插槽 >> 索引序号, 查询是通过插槽找到索引位置,如果key相同,更新插槽的索引位置值
                // 如果当前索引是IndexFile的第一个索引
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                // 更新IndexHeader的信息
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    /**
     * 索引文件的起始和终止时间与给定的时间有交集
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 上述设计，可以支持 hashcode 冲突，，多个不同的key,相同的 hashcode,index 条目其实是一个逻辑链表的概念，因为每个index 条目的最后4个字节存放的就是上一个的位置。
     * 知道存了储结构，要检索 index文件就变的简单起来来，其实就根据 key 得到 hashcode,然后从最新的条目开始找，匹配时间戳是否有效，
     * 得到消息的物理地址（存放在commitlog文件中），然后就可以根据 commitlog 偏移量找到具体的消息，从而得到最终的key-value。
     *      提取符合key和topic的Message的PhyOffset
     *      @param key        topic#key
     *      @param lock       查询最后一个indexFile时锁定,防止阻止其继续创建索引
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                // index的序号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
