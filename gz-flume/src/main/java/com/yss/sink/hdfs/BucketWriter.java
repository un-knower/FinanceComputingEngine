/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yss.sink.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.yss.sink.hdfs.HDFSEventSink.WriterCallback;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal API intended for HDFSSink use.
 * This class does file rolling and handles file formats and serialization.
 * Only the public methods in this class are thread safe.
 */
class BucketWriter {

    private static final Logger LOG = LoggerFactory
            .getLogger(BucketWriter.class);

    /**
     * This lock ensures that only one thread can open a file at a time.
     */
    private static final Integer staticLock = new Integer(1);
    private Method isClosedMethod = null;

    private HDFSWriter writer;
    private final long rollInterval;
    private final long rollSize;
    private final long rollCount;
    private final long batchSize;
    private final CompressionCodec codeC;
    private final CompressionType compType;
    private final ScheduledExecutorService timedRollerPool;
    private final PrivilegedExecutor proxyUser;

    private final AtomicLong fileExtensionCounter;

    private long eventCounter;
    private long processSize;

    private FileSystem fileSystem;

    private volatile String filePath;
    private volatile String fileName;
    private volatile String inUsePrefix;
    private volatile String inUseSuffix;
    private volatile String fileSuffix;
    private volatile String bucketPath;
    private volatile String targetPath;
    private volatile long batchCounter;
    private volatile boolean isOpen;
    private volatile boolean isUnderReplicated;
    private volatile int consecutiveUnderReplRotateCount = 0;
    private volatile ScheduledFuture<Void> timedRollFuture;
    private SinkCounter sinkCounter;
    private final int idleTimeout;
    private volatile ScheduledFuture<Void> idleFuture;
    private final WriterCallback onCloseCallback;
    private final String onCloseCallbackPath;
    private final long callTimeout;
    private final ExecutorService callTimeoutPool;
    private final int maxConsecUnderReplRotations = 30; // make this config'able?

    private boolean mockFsInjected = false;

    private final long retryInterval;
    private final int maxRenameTries;

    // flag that the bucket writer was closed due to idling and thus shouldn't be
    // reopened. Not ideal, but avoids internals of owners
    protected boolean closed = false;
    AtomicInteger renameTries = new AtomicInteger(0);

    BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
                 Context context, String filePath, String fileName, String inUsePrefix,
                 String inUseSuffix, String fileSuffix, CompressionCodec codeC,
                 CompressionType compType, HDFSWriter writer,
                 ScheduledExecutorService timedRollerPool, PrivilegedExecutor proxyUser,
                 SinkCounter sinkCounter, int idleTimeout, WriterCallback onCloseCallback,
                 String onCloseCallbackPath, long callTimeout,
                 ExecutorService callTimeoutPool, long retryInterval,
                 int maxCloseTries) {
        this(rollInterval, rollSize, rollCount, batchSize,
                context, filePath, fileName, inUsePrefix,
                inUseSuffix, fileSuffix, codeC,
                compType, writer,
                timedRollerPool, proxyUser,
                sinkCounter, idleTimeout, onCloseCallback,
                onCloseCallbackPath, callTimeout,
                callTimeoutPool, retryInterval,
                maxCloseTries, new SystemClock());
    }

    BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
                 Context context, String filePath, String fileName, String inUsePrefix,
                 String inUseSuffix, String fileSuffix, CompressionCodec codeC,
                 CompressionType compType, HDFSWriter writer,
                 ScheduledExecutorService timedRollerPool, PrivilegedExecutor proxyUser,
                 SinkCounter sinkCounter, int idleTimeout, WriterCallback onCloseCallback,
                 String onCloseCallbackPath, long callTimeout,
                 ExecutorService callTimeoutPool, long retryInterval,
                 int maxCloseTries, Clock clock) {
        this.rollInterval = rollInterval;
        this.rollSize = rollSize;
        this.rollCount = rollCount;
        this.batchSize = batchSize;
        this.filePath = filePath;
        this.fileName = fileName;
        this.inUsePrefix = inUsePrefix;
        this.inUseSuffix = inUseSuffix;
        this.fileSuffix = fileSuffix;
        this.codeC = codeC;
        this.compType = compType;
        this.writer = writer;
        this.timedRollerPool = timedRollerPool;
        this.proxyUser = proxyUser;
        this.sinkCounter = sinkCounter;
        this.idleTimeout = idleTimeout;
        this.onCloseCallback = onCloseCallback;
        this.onCloseCallbackPath = onCloseCallbackPath;
        this.callTimeout = callTimeout;
        this.callTimeoutPool = callTimeoutPool;
        fileExtensionCounter = new AtomicLong(clock.currentTimeMillis());

        this.retryInterval = retryInterval;
        this.maxRenameTries = maxCloseTries;
        isOpen = false;
        isUnderReplicated = false;
        this.writer.configure(context);
    }

    @VisibleForTesting
    void setFileSystem(FileSystem fs) {
        this.fileSystem = fs;
        mockFsInjected = true;
    }

    @VisibleForTesting
    void setMockStream(HDFSWriter dataWriter) {
        this.writer = dataWriter;
    }


    /**
     * Clear the class counters
     */
    private void resetCounters() {
        eventCounter = 0;
        processSize = 0;
        batchCounter = 0;
    }

    private Method getRefIsClosed() {
        try {
            //文件关闭
            return fileSystem.getClass().getMethod("isFileClosed",
                    Path.class);
        } catch (Exception e) {
            LOG.info("isFileClosed() is not available in the version of the " +
                    "distributed filesystem being used. " +
                    "Flume will not attempt to re-close files if the close fails " +
                    "on the first attempt");
            return null;
        }
    }

    private Boolean isFileClosed(FileSystem fs, Path tmpFilePath) throws Exception {
        return (Boolean) (isClosedMethod.invoke(fs, tmpFilePath));
    }

    /**
     * open() is called by append()
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void open() throws IOException, InterruptedException {
        if ((filePath == null) || (writer == null)) {
            throw new IOException("Invalid file settings");
        }

        final Configuration config = new Configuration();
        // 禁用文件系统JVM关机钩
        config.setBoolean("fs.automatic.close", false);

        // 在执行某些RPC操作时，Hadoop并不是线程安全的，
        //在Kerberos之下运行时，包括get文件系统（）。
        //open（）必须在JVM中一次被一个线程调用。
        //注意：以前尝试在底层Kerberos主体上同步
        //造成死锁。看到水槽- 1231。
        synchronized (staticLock) {
            checkAndThrowInterruptedException();

            try {
                long counter = fileExtensionCounter.incrementAndGet();
                //文件名字中的数值
//                String fullFileName = fileName + "." + counter;
                String fullFileName = fileName;

                if (fileSuffix != null && fileSuffix.length() > 0) {
                    fullFileName += fileSuffix;
                } else if (codeC != null) {
                    fullFileName += codeC.getDefaultExtension();
                }
                bucketPath = filePath + "/" + inUsePrefix
                        + fullFileName + inUseSuffix;
                targetPath = filePath + "/" + fullFileName;

                LOG.info("Creating " + bucketPath);
                //暂停
                callWithTimeout(new CallRunner<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (codeC == null) {
                            // 需要在底层使用上述配置来引用FS
                            //作者为了避免关闭钩子和非法的州例外
                            if (!mockFsInjected) {
                                fileSystem = new Path(bucketPath).getFileSystem(config);
                            }
                            writer.open(bucketPath);
                        } else {
                            //在写作者之前需要参考FS
                            //避免关闭钩子
                            if (!mockFsInjected) {
                                fileSystem = new Path(bucketPath).getFileSystem(config);
                            }
                            writer.open(bucketPath, codeC, compType);
                        }
                        return null;
                    }
                });
            } catch (Exception ex) {
                sinkCounter.incrementConnectionFailedCount();
                if (ex instanceof IOException) {
                    throw (IOException) ex;
                } else {
                    throw Throwables.propagate(ex);
                }
            }
        }
        isClosedMethod = getRefIsClosed();
        sinkCounter.incrementConnectionCreatedCount();
        resetCounters();

        // 如果启用了基于时间的滚动，请安排滚动
        if (rollInterval > 0) {
            Callable<Void> action = new Callable<Void>() {
                public Void call() throws Exception {
                    LOG.debug("Rolling file ({}): Roll scheduled after {} sec elapsed.",
                            bucketPath, rollInterval);
                    try {
                        // Roll the file and remove reference from sfWriters map.
                        close(true);
                    } catch (Throwable t) {
                        LOG.error("Unexpected error", t);
                    }
                    return null;
                }
            };
            timedRollFuture = timedRollerPool.schedule(action, rollInterval,
                    TimeUnit.SECONDS);
        }

        isOpen = true;
    }

    /**
     * 关闭文件句柄并将临时文件重命名为永久文件名。
     * 安全多次呼叫。异常日志HDFSWriter.close()。这
     * 方法不会使bucket编写器从HDFS中被取消引用。
     * 拥有它的水槽。这个方法应该只在大小或计数时使用。
     * 基于滚动关闭这个文件。
     *
     * @throws IOException          如果临时文件存在，则无法重命名。
     * @throws InterruptedException
     */
    public synchronized void close() throws IOException, InterruptedException {
        close(false);
    }

    private CallRunner<Void> createCloseCallRunner() {
        return new CallRunner<Void>() {
            private final HDFSWriter localWriter = writer;

            @Override
            public Void call() throws Exception {
                localWriter.close(); // could block
                return null;
            }
        };
    }

    private Callable<Void> createScheduledRenameCallable() {

        return new Callable<Void>() {
            private final String path = bucketPath;
            private final String finalPath = targetPath;
            private FileSystem fs = fileSystem;
            private int renameTries = 1; // 一次尝试已经完成了

            @Override
            public Void call() throws Exception {
                if (renameTries >= maxRenameTries) {
                    LOG.warn("Unsuccessfully attempted to rename " + path + " " +
                            maxRenameTries + " times. File may still be open.");
                    return null;
                }
                renameTries++;
                try {
                    renameBucket(path, finalPath, fs);
                } catch (Exception e) {
                    LOG.warn("Renaming file: " + path + " failed. Will " +
                            "retry again in " + retryInterval + " seconds.", e);
                    timedRollerPool.schedule(this, retryInterval, TimeUnit.SECONDS);
                    return null;
                }
                return null;
            }
        };
    }

    /**
     * 试着为当前的bucketPath启动租赁恢复过程
     * 如果文件系统是分布式文件系统。
     * 捕获并记录IOException。
     */
    private synchronized void recoverLease() {
        if (bucketPath != null && fileSystem instanceof DistributedFileSystem) {
            try {
                LOG.debug("Starting lease recovery for {}", bucketPath);
                ((DistributedFileSystem) fileSystem).recoverLease(new Path(bucketPath));
            } catch (IOException ex) {
                LOG.warn("Lease recovery failed for {}", bucketPath, ex);
            }
        }
    }

    /**
     * 关闭文件句柄并将临时文件重命名为永久文件名。
     * 安全多次呼叫。异常日志HDFSWriter.close()。
     *
     * @throws IOException          如果临时文件存在，则无法重命名。
     * @throws InterruptedException
     */
    public synchronized void close(boolean callCloseCallback)
            throws IOException, InterruptedException {
        checkAndThrowInterruptedException();
        try {
            flush();
        } catch (IOException e) {
            LOG.warn("pre-close flush failed", e);
        }

        LOG.info("Closing {}", bucketPath);
        CallRunner<Void> closeCallRunner = createCloseCallRunner();
        if (isOpen) {
            try {
                //暂停
                callWithTimeout(closeCallRunner);
                sinkCounter.incrementConnectionClosedCount();
            } catch (IOException e) {
                LOG.warn("failed to close() HDFSWriter for file (" + bucketPath +
                        "). Exception follows.", e);
                sinkCounter.incrementConnectionFailedCount();
                // starting lease recovery process, see FLUME-3080
                recoverLease();
            }
            isOpen = false;
        } else {
            LOG.info("HDFSWriter is already closed: {}", bucketPath);
        }

        // NOTE: 定时滚动通过这个codepath和其他滚动类型
        //根据时间进行滚动
        if (timedRollFuture != null && !timedRollFuture.isDone()) {
            timedRollFuture.cancel(false); // 如果跑步，不要取消我自己！
            timedRollFuture = null;
        }
        //HDFS操作允许的毫秒数，例如打开、写入、刷新、关闭
        //如果idleFuture不为null或者这个任务没有完成
        if (idleFuture != null && !idleFuture.isDone()) {
            idleFuture.cancel(false); // do not cancel myself if running!
            idleFuture = null;
        }

        if (bucketPath != null && fileSystem != null) {
            // could block or throw IOException
            try {
                renameBucket(bucketPath, targetPath, fileSystem);
            } catch (Exception e) {
                LOG.warn("failed to rename() file (" + bucketPath +
                        "). Exception follows.", e);
                sinkCounter.incrementConnectionFailedCount();
                final Callable<Void> scheduledRename = createScheduledRenameCallable();
                timedRollerPool.schedule(scheduledRename, retryInterval, TimeUnit.SECONDS);
            }
        }
        if (callCloseCallback) {
            runCloseAction();
            closed = true;
        }
    }

    /**
     * flush the data
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public synchronized void flush() throws IOException, InterruptedException {
        checkAndThrowInterruptedException();
        //批处理完成
        if (!isBatchComplete()) {
            doFlush();

            if (idleTimeout > 0) {
                // 如果未来存在并且不能被取消，那就意味着它已经运行或被取消了
                if (idleFuture == null || idleFuture.cancel(false)) {
                    Callable<Void> idleAction = new Callable<Void>() {
                        public Void call() throws Exception {
                            LOG.info("Closing idle bucketWriter {} at {}", bucketPath,
                                    System.currentTimeMillis());
                            if (isOpen) {
                                close(true);
                            }
                            return null;
                        }
                    };
                    idleFuture = timedRollerPool.schedule(idleAction, idleTimeout,
                            TimeUnit.SECONDS);
                }
            }
        }
    }

    //运行关闭删除hashMap表中的数据
    private void runCloseAction() {
        System.out.println("文件上传HDFS完成当前时间是:" + System.currentTimeMillis());
        try {
            if (onCloseCallback != null) {
                onCloseCallback.run(onCloseCallbackPath);
            }
        } catch (Throwable t) {
            LOG.error("意外的错误", t);
        }
    }

    /**
     * doFlush() must only be called by flush()
     *
     * @throws IOException
     */
    private void doFlush() throws IOException, InterruptedException {
        callWithTimeout(new CallRunner<Void>() {
            @Override
            public Void call() throws Exception {
                writer.sync(); // could block
                return null;
            }
        });
        batchCounter = 0;
    }

    /**
     * 打开文件句柄，写入数据，更新状态，处理文件滚动
     * 批处理/冲洗。< br / >
     * 如果写失败，文件被隐式关闭，然后是IOException
     * * rethrown。< br / >
     * 我们在append之前旋转，而不是之后，所以活动文件滚动
     * 机制永远不会滚动一个空文件。这也确保了文件
     * 创建时间反映了第一个事件被写入的时间。
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public synchronized void append(final Event event)
            throws IOException, InterruptedException {
        checkAndThrowInterruptedException();
        // 如果idleFuture不是空的，在我们前进之前取消它，以避免
        ////在附加的中间关闭呼叫。
        if (idleFuture != null) {
            idleFuture.cancel(false);
            // 仍然有一个小的种族条件——如果idleFuture已经是
            ////运行，中断它会导致HDFS关闭操作-
            ////所以我们不能在跑步时打断它。如果未来不可能
            ////取消了，它已经在运行了-等它完成之前
            ////试图写作。
            if (!idleFuture.isDone()) {
                try {
                    //HDFS操作允许的毫秒数，例如打开、写入、刷新、关闭
                    idleFuture.get(callTimeout, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ex) {
                    LOG.warn("Timeout while trying to cancel closing of idle file. Idle" +
                            " file close may have failed", ex);
                } catch (Exception ex) {
                    LOG.warn("Error while trying to cancel closing of idle file. ", ex);
                }
            }
            idleFuture = null;
        }

        // 如果bucket编写者由于滚动超时或空闲超时而关闭，
        //强迫一个新的bucket编写器被创建。卷数和卷大小
        //重复使用这个
        if (!isOpen) {
            if (closed) {
                throw new BucketClosedException("This bucket writer was closed and " +
                        "this handle is thus no longer valid");
            }
            open();
        }

        // 检查是否该滚动文件
        if (shouldRotate()) {
            boolean doRotate = true;

            if (isUnderReplicated) {
                if (maxConsecUnderReplRotations > 0 &&
                        consecutiveUnderReplRotateCount >= maxConsecUnderReplRotations) {
                    doRotate = false;
                    if (consecutiveUnderReplRotateCount == maxConsecUnderReplRotations) {
                        LOG.error("Hit max consecutive under-replication rotations ({}); " +
                                "will not continue rolling files under this path due to " +
                                "under-replication", maxConsecUnderReplRotations);
                    }
                } else {
                    LOG.warn("Block Under-replication detected. Rotating file.");
                }
                consecutiveUnderReplRotateCount++;
            } else {
                consecutiveUnderReplRotateCount = 0;
            }

            if (doRotate) {
                close();
                open();
            }
        }

        // 写事件
        try {
            sinkCounter.incrementEventDrainAttemptCount();
            callWithTimeout(new CallRunner<Void>() {
                @Override
                public Void call() throws Exception {
                    writer.append(event); // could block
                    return null;
                }
            });
        } catch (IOException e) {
            LOG.warn("Caught IOException writing to HDFSWriter ({}). Closing file (" +
                            bucketPath + ") and rethrowing exception.",
                    e.getMessage());
            try {
                close(true);
            } catch (IOException e2) {
                LOG.warn("Caught IOException while closing file (" +
                        bucketPath + "). Exception follows.", e2);
            }
            throw e;
        }

        // update statistics
        processSize += event.getBody().length;
        eventCounter++;
        batchCounter++;

        if (batchCounter == batchSize) {
            flush();
        }
    }

    /**
     * 检查是否有根据事件的数量旋转文件
     * 文件大小以字节为单位，以字节为单位
     */
    private boolean shouldRotate() {
        boolean doRotate = false;

        if (writer.isUnderReplicated()) {
            this.isUnderReplicated = true;
            doRotate = true;
        } else {
            this.isUnderReplicated = false;
        }
        //根据事件的数量
        if ((rollCount > 0) && (rollCount <= eventCounter)) {
            LOG.debug("rolling: rollCount: {}, events: {}", rollCount, eventCounter);
            doRotate = true;
        }
        //根据文件的大小
        if ((rollSize > 0) && (rollSize <= processSize)) {
            LOG.debug("rolling: rollSize: {}, bytes: {}", rollSize, processSize);
            doRotate = true;
        }

        return doRotate;
    }

    /**
     * 将bucketPath文件重命名为.tmp到永久位置。
     */
    // 当这个bucket编写器基于rollCount或
    ////rollSize，同一个实例被重新用于新文件。但是,如果
    ////以前的文件没有关闭/重命名，
    ////桶写作者字段不再指向它，因此需要
    ////从线程中传递过来，试图关闭它。甚至
    ////当bucket编写者由于关闭超时而关闭时，
    ////这个方法可以从预定的线程中调用
    ////文件稍后会被关闭——因此隐含的引用
    ////桶写作者仍然可以在可调用的实例中存活。
    private void renameBucket(String bucketPath, String targetPath, final FileSystem fs)
            throws IOException, InterruptedException {
        if (bucketPath.equals(targetPath)) {
            return;
        }

        final Path srcPath = new Path(bucketPath);
        final Path dstPath = new Path(targetPath);

        callWithTimeout(new CallRunner<Void>() {
            @Override
            public Void call() throws Exception {
                if (fs.exists(srcPath)) { // could block
                    LOG.info("Renaming " + srcPath + " to " + dstPath);
                    renameTries.incrementAndGet();
                    fs.rename(srcPath, dstPath); // could block
                }
                return null;
            }
        });
    }

    @Override
    public String toString() {
        return "[ " + this.getClass().getSimpleName() + " targetPath = " + targetPath +
                ", bucketPath = " + bucketPath + " ]";
    }

    private boolean isBatchComplete() {
        return (batchCounter == 0);
    }

    /**
     * This method if the current thread has been interrupted and throws an
     * exception.
     *
     * @throws InterruptedException
     */
    private static void checkAndThrowInterruptedException()
            throws InterruptedException {
        if (Thread.currentThread().interrupted()) {
            throw new InterruptedException("Timed out before HDFS call was made. "
                    + "Your hdfs.callTimeout might be set too low or HDFS calls are "
                    + "taking too long.");
        }
    }

    /**
     * Execute the callable on a separate thread and wait for the completion
     * for the specified amount of time in milliseconds. In case of timeout
     * cancel the callable and throw an IOException
     */
    private <T> T callWithTimeout(final CallRunner<T> callRunner)
            throws IOException, InterruptedException {
        Future<T> future = callTimeoutPool.submit(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return proxyUser.execute(new PrivilegedExceptionAction<T>() {
                    @Override
                    public T run() throws Exception {
                        return callRunner.call();
                    }
                });
            }
        });
        try {
            if (callTimeout > 0) {
                return future.get(callTimeout, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (TimeoutException eT) {
            future.cancel(true);
            sinkCounter.incrementConnectionFailedCount();
            throw new IOException("Callable timed out after " +
                    callTimeout + " ms" + " on file: " + bucketPath, eT);
        } catch (ExecutionException e1) {
            sinkCounter.incrementConnectionFailedCount();
            Throwable cause = e1.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw new RuntimeException(e1);
            }
        } catch (CancellationException ce) {
            throw new InterruptedException(
                    "Blocked callable interrupted by rotation event");
        } catch (InterruptedException ex) {
            LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * Simple interface whose <tt>call</tt> method is called by
     * {#callWithTimeout} in a new thread inside a
     * {@linkplain PrivilegedExceptionAction#run()} call.
     *
     * @param <T>
     */
    private interface CallRunner<T> {
        T call() throws Exception;
    }

}
