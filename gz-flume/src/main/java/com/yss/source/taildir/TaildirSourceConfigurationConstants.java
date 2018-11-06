/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.yss.source.taildir;

public class TaildirSourceConfigurationConstants {
    /**
     * Mapping for tailing file groups.
     */
    public static final String FILE_GROUPS = "filegroups";
    public static final String FILE_GROUPS_PREFIX = FILE_GROUPS + ".";

    /**
     * Mapping for putting headers to events grouped by file groups.
     */
    public static final String HEADERS_PREFIX = "headers.";

    /**
     * Path of position file.
     */
    public static final String POSITION_FILE = "positionFile";
    public static final String DEFAULT_POSITION_FILE = "/.flume/taildir_position.json";

    /**
     * What size to batch with before sending to ChannelProcessor.
     */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    /**
     * Whether to skip the position to EOF in the case of files not written on the position file.
     */
    public static final String SKIP_TO_END = "skipToEnd";
    public static final boolean DEFAULT_SKIP_TO_END = false;

    /**
     * Time (ms) to close idle files.
     */
    public static final String IDLE_TIMEOUT = "idleTimeout";
    public static final int DEFAULT_IDLE_TIMEOUT = 120000;

    /**
     * Interval time (ms) to write the last position of each file on the position file.
     */
    public static final String WRITE_POS_INTERVAL = "writePosInterval";
    public static final int DEFAULT_WRITE_POS_INTERVAL = 3000;

    /**
     * Whether to add the byte offset of a tailed line to the header
     */
    public static final String BYTE_OFFSET_HEADER = "byteOffsetHeader";
    public static final String BYTE_OFFSET_HEADER_KEY = "byteoffset";
    public static final boolean DEFAULT_BYTE_OFFSET_HEADER = false;

    /**
     * Whether to cache the list of files matching the specified file patterns till parent directory
     * is modified.
     */
    public static final String CACHE_PATTERN_MATCHING = "cachePatternMatching";
    public static final boolean DEFAULT_CACHE_PATTERN_MATCHING = true;

    /**
     * Header in which to put absolute path filename.
     */
    public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final String DEFAULT_FILENAME_HEADER_KEY = "fileName";

    /**
     * Whether to include absolute path filename in a header.
     */
    public static final String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = true;


    /**
     * 读取Xml文件设置key值
     */
    public static final String XML_NODE = "xmlNode";
    public static final String DEFAULT_XML_NODE = "Security";

    /**
     * 在Event的Headers头中每行记录对应的key值
     */
    public static final String CURRENT_RECORD = "currentLine";
    public static final String DEFAULT_CURRENT_RECORD = "currentRecord";

    /**
     * 在Event的Headers头中每行记录对应的key值
     */
    public static final String SEPARATOR = "csvSeparator";
    public static final String DEFAULT_SEPARATOR = "\t";

    /**
     * 在监控的目录里是否根据时间排除过期的目录
     */
    public static final String DIRECTORY_DATE = "directoryDate";
    public static final boolean DEFAULT_DIRECTORY_DATE = true;

    /**
     * 文件读完后是否修改文件名
     */
    public static final String RENAME_FLIE = "renameFlie";
    public static final boolean DEFAULT_RENAME_FLIE = false;


    /**
     * 每个Event包含行多少行的数据
     */
    public static final String EVENT_LINES = "eventLines";
    public static final int DEFAULT_EVENT_LINES = 50;


    /**
     * 是否发送头文件HEAD
     */
    public static final String HEAD = "headFile";
    public static final boolean DEFAULT_HEAD = true;


    /**
     * 按文件名的前缀过滤文件,默认值必须有,但是可以是任意值最好是永远不会出现的值
     */
    public static final String PREFIXSTR = "prefixStr";
    public static final String DEFAULT_PREFIXSTR = "doesntfdjfskfksdfkjsfsfskjkfsjdfsj";

    /**
     * 原始文件内容分隔符是@
     * <p>
     * A  代表  @
     * B  代表 |
     */
    public static final String SOURCESEPARATOR_A = "sourceA";
    public static final String DEFAULT_SOURCESEPARATOR_A = "@";
    /**
     * 原始文件内容分隔符是|
     * <p>
     * A  代表  @
     * B  代表 |
     */
    public static final String SOURCE_SEPARATOR_B = "sourceB";
    public static final String DEFAULT_SOURCE_SEPARATOR_B = "\\|";


    /**
     * 原始文件名是@正则表达式
     * <p>
     * A  代表  @
     * B  代表 |
     */
    public static final String SOURCE_REGEX_A = "regexA";
    public static final String DEFAULT_SOURCE_REGEX_A = "^\\d*(delivdetails|holddata|trddata|otherfund|fundchg|cusfund)\\d*\\.txt$";

    /**
     * 原始文件名是|正则表达式
     * <p>
     * A  代表  @
     * B  代表 |
     */
    public static final String SOURCE_REGEX_B = "regexB";
    public static final String DEFAULT_SOURCE_REGEX_B = "(^[a-z].*\\d{6}[a-z].*\\d*00(SPOTMATCH|DEFERMATCH|DEFERDELIVERYAPPMATCH|CLIENTSTORAGEFLOW|ETFAPPLY|MEMBERSEATCAPICATL|CLIENTMISFEEDETAIL|CLIENTSTORAGE|LARGEAMOUNTMATCH)\\.txt$)|(^hkex(clpr|reff)04_\\d*\\.txt$)|(^reff0[4|3]\\d*\\.txt$)|(^cpxx\\d*\\.txt$)";
    /**
     * 原始文件名是字节正则表达式
     */
    public static final String SOURCE_REGEX_FSD_FOUR = "regexFsdFour";
    public static final String DEFAULT_SOURCE_REGEX_FSD_FOUR = "^FSD_\\d*_\\d*_\\d*_04_\\d*_\\d*\\.txt$";
    /**
     * 字节数组
     */
    public static final String SOURCE_FSD_FOUR_BYTES = "fsdFourBytes";
    public static final String DEFAULT_SOURCE_FSD_FOUR_BYTES = "24,24,8,3,16,16,6,1,8,6,4,17,9,16,16,3,12,20,1,5,4,10,10,7,9,24,8,10,1,16,9,16,20,60,9,9,17,4,1,16,10,16,16,7,8,1,16,7,7,16,16,8,10,1,1,1,1,8,1,6,10,12,2,9,1,1,1,2,2,5,6,7,16,16,16,16,1,20,16,1,16,16,16,60,16,16,12,12,12,18,18";

    /**
     * 原始文件名是字节正则表达式
     */
    public static final String SOURCE_REGEX_FSD_SIX = "regexFsdSix";
    public static final String DEFAULT_SOURCE_REGEX_FSD_SIX = "^FSD_\\d*_\\d*_\\d*_06_\\d*_\\d*\\.txt$";

    /**
     * 字节数组
     */
    public static final String SOURCE_FSD_SIX_BYTES = "fsdSixBytes";
    public static final String DEFAULT_SOURCE_FSD_SIX_BYTES = "16,8,3,16,8,16,8,16,6,8,4,17,9,3,12,16,1,4,8,10,10,16,7,9,10,16,1,16,20,16,16,10,1,1,10,16,1,24,16,16,12,12,12,18,18";
    /**
     * 原始文件名是字节正则表达式
     */
    public static final String SOURCE_REGEX_FSD_JY = "regexFsdJY";
    public static final String DEFAULT_SOURCE_REGEX_FSD_JY = "^FSD_\\d*_\\d*_\\d*_JY\\.TXT$";

    /**
     * 字节数组
     */
    public static final String SOURCE_FSD_JY_BYTES = "fsdJYBytes";
    public static final String DEFAULT_SOURCE_FSD_JY_BYTES  = "64,3,24,24,12,12,12,1,8,6,1,1,80,6,16,16,3,9,9,17,12,2,60,6,1,1,1,1,1,6,120,80,30,80,20,22,24,40,24,20,30,1,8,120,6,22,24,40,24,20,30,1,8,120,6,120,80,30,60,20,22,24,40,24,20,18,17,12,8,24,8,20,1,2,40,3,4,1,60,20,20,30,16,16";


}
