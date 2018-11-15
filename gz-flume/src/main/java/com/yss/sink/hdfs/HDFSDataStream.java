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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;

public class HDFSDataStream extends AbstractHDFSWriter {

    private static final Logger logger = LoggerFactory.getLogger(HDFSDataStream.class);

    private FSDataOutputStream outStream;
    private String serializerType;
    private Context serializerContext;
    private EventSerializer serializer;
    private boolean useRawLocalFileSystem;

    @Override
    public void configure(Context context) {
        super.configure(context);

        serializerType = context.getString("serializer", "TEXT");
        useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
                false);
        serializerContext =
                new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
        logger.info("Serializer = " + serializerType + ", UseRawLocalFileSystem = "
                + useRawLocalFileSystem);
    }

    @VisibleForTesting
    protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
        return dstPath.getFileSystem(conf);
    }

    protected void doOpen(Configuration conf, Path dstPath, FileSystem hdfs) throws IOException {
        if (useRawLocalFileSystem) {
            if (hdfs instanceof LocalFileSystem) {
                hdfs = ((LocalFileSystem) hdfs).getRaw();
            } else {
                logger.warn("useRawLocalFileSystem is set to true but file system " +
                        "is not of type LocalFileSystem: " + hdfs.getClass().getName());
            }
        }

        boolean appending = false;
        if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile(dstPath)) {
            outStream = hdfs.append(dstPath);
            appending = true;
        } else {
            //删除已存在的文件
            Path parent = dstPath.getParent();
//            if (parent.getName().equals(LocalDate.now().toString().replaceAll("-", ""))) {
                String filePath = dstPath.toString();
                String path = filePath.substring(0, filePath.length() - 4);
                Path fs = new Path(path);
                if (hdfs.exists(fs)) {
                    hdfs.delete(fs, false);
                }
//            } else {
//                if (hdfs.exists(parent)) {
                    hdfs.delete(parent, true);
//                }
//            }
            logger.info("HDFS创建文件开始写数据:" + dstPath.toString() + "    当前时间是:" + System.currentTimeMillis());
            outStream = hdfs.create(dstPath);
        }

        serializer = EventSerializerFactory.getInstance(
                serializerType, serializerContext, outStream);
        if (appending && !serializer.supportsReopen()) {
            outStream.close();
            serializer = null;
            throw new IOException("serializer (" + serializerType +
                    ") does not support append");
        }

        // 必须调用超类来检查复制问题
        registerCurrentStream(outStream, hdfs, dstPath);

        if (appending) {
            serializer.afterReopen();
        } else {
            serializer.afterCreate();
        }
    }

    @Override
    public void open(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path dstPath = new Path(filePath);
        FileSystem hdfs = getDfs(conf, dstPath);
        doOpen(conf, dstPath, hdfs);
    }

    @Override
    public void open(String filePath, CompressionCodec codec,
                     CompressionType cType) throws IOException {
        open(filePath);
    }

    @Override
    public void append(Event e) throws IOException {
        serializer.write(e);
    }

    @Override
    public void sync() throws IOException {
        serializer.flush();
        outStream.flush();
        hflushOrSync(outStream);
    }

    @Override
    public void close() throws IOException {
        serializer.flush();
        serializer.beforeClose();
        outStream.flush();
        hflushOrSync(outStream);
        outStream.close();

        unregisterCurrentStream();
    }

}
