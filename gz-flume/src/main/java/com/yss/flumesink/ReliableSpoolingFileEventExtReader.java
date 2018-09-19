//package com.yss.flumesink;//package com.yss.flumesink;
//
//import com.google.common.base.Charsets;
//import com.google.common.base.Optional;
//
//import com.google.common.base.Preconditions;
//import com.google.common.io.Files;
//import org.apache.flume.Context;
//import org.apache.flume.Event;
//import org.apache.flume.FlumeException;
//import org.apache.flume.client.avro.ReliableEventReader;
//import org.apache.flume.serialization.DecodeErrorPolicy;
//
//import org.apache.flume.serialization.EventDeserializer;
//
//import org.joda.time.DateTime;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.charset.Charset;
//import java.util.Collections;
//import java.util.List;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
///**
// * @author yupan
// * 2018-08-28 10:11
// */
//public class ReliableSpoolingFileEventExtReader implements ReliableEventReader {
//
//    private static final Logger logger = LoggerFactory.getLogger(ReliableSpoolingFileEventExtReader.class);
//
//    static final String metaFileName = ".flumespool-main.meta";
//    private final File spoolDirectory;
//    private final String completedSuffix;
//    private final String deserializerType;
//    private final Context deserializerContext;
//    private final Pattern ignorePattern;
//    private final File metaFile;
//    private final boolean annotateFileName;
//    private final boolean annotateBaseName;
//    private final String fileNameHeader;
//    private final String baseNameHeader;
//
//    private final boolean annotateFileNameExtractor;
//    private final String fileNameExtractorHeader;
//    private final Pattern fileNameExtractorPattern;
//    private final boolean convertToTimestamp;
//    private final String dateTimeFormat;
//
//    private final boolean splitFileName;
//    private final String splitBy;
//    private final String splitBaseNameHeader;
//
//
//    private final String deletePolicy;
//    private final Charset inputCharset;
//    private final DecodeErrorPolicy decodeErrorPolicy;
//
//    private Optional<FileInfo> currentFile = Optional.absent();
//    private Optional<FileInfo> lastFileRead = Optional.absent();
//    private boolean committed = true;
//
//    private ReliableSpoolingFileEventExtReader(File spoolDirectory,
//                                               String completedSuffix, String ignorePattern,
//                                               String trackerDirPath, boolean annotateFileName,
//                                               String fileNameHeader, boolean annotateBaseName,
//                                               String baseNameHeader, String deserializerType,
//                                               Context deserializerContext, String deletePolicy,
//                                               String inputCharset, DecodeErrorPolicy decodeErrorPolicy,
//                                               boolean annotateFileNameExtractor, String fileNameExtractorHeader,
//                                               String fileNameExtractorPattern, boolean convertToTimestamp,
//                                               String dateTimeFormat, boolean splitFileName, String splitBy,
//                                               String splitBaseNameHeader)throws IOException {
//
//
//        Preconditions.checkNotNull(spoolDirectory);
//        Preconditions.checkNotNull(completedSuffix);
//        Preconditions.checkNotNull(ignorePattern);
//        Preconditions.checkNotNull(trackerDirPath);
//        Preconditions.checkNotNull(deserializerType);
//        Preconditions.checkNotNull(deserializerContext);
//        Preconditions.checkNotNull(deletePolicy);
//        Preconditions.checkNotNull(inputCharset);
//
//        if (!deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())
//                && !deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
//            throw new IllegalArgumentException("Delete policies other than "
//                    + "NEVER and IMMEDIATE are not yet supported");
//        }
//
//        if (logger.isDebugEnabled()) {
//            logger.debug("Initializing {} with directory={}, metaDir={}, "
//                    + "deserializer={}", new Object[] {
//                    ReliableSpoolingFileEventExtReader.class.getSimpleName(),
//                    spoolDirectory, trackerDirPath, deserializerType });
//        }
//
//        Preconditions
//                .checkState(
//                        spoolDirectory.exists(),
//                        "Directory does not exist: "
//                                + spoolDirectory.getAbsolutePath());
//        Preconditions.checkState(spoolDirectory.isDirectory(),
//                "Path is not a directory: " + spoolDirectory.getAbsolutePath());
//
//        try {
//            File canary = File.createTempFile("flume-spooldir-perm-check-",
//                    ".canary", spoolDirectory);
//            Files.write("testing flume file permissions\n", canary,
//                    Charsets.UTF_8);
//            List<String> lines = Files.readLines(canary, Charsets.UTF_8);
//            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s",
//                    canary);
//            if (!canary.delete()) {
//                throw new IOException("Unable to delete canary file " + canary);
//            }
//            logger.debug("Successfully created and deleted canary file: {}",
//                    canary);
//        } catch (IOException e) {
//            throw new FlumeException("Unable to read and modify files"
//                    + " in the spooling directory: " + spoolDirectory, e);
//        }
//
//        this.spoolDirectory = spoolDirectory;
//        this.completedSuffix = completedSuffix;
//        this.deserializerType = deserializerType;
//        this.deserializerContext = deserializerContext;
//        this.annotateFileName = annotateFileName;
//        this.fileNameHeader = fileNameHeader;
//        this.annotateBaseName = annotateBaseName;
//        this.baseNameHeader = baseNameHeader;
//        this.ignorePattern = Pattern.compile(ignorePattern);
//        this.deletePolicy = deletePolicy;
//        this.inputCharset = Charset.forName(inputCharset);
//        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
//
//        // 增加代码开始
//        this.annotateFileNameExtractor = annotateFileNameExtractor;
//        this.fileNameExtractorHeader = fileNameExtractorHeader;
//        this.fileNameExtractorPattern = Pattern
//                .compile(fileNameExtractorPattern);
//        this.convertToTimestamp = convertToTimestamp;
//        this.dateTimeFormat = dateTimeFormat;
//
//        this.splitFileName = splitFileName;
//        this.splitBy = splitBy;
//        this.splitBaseNameHeader = splitBaseNameHeader;
//        // 增加代码结束
//
//        File trackerDirectory = new File(trackerDirPath);
//
//        // if relative path, treat as relative to spool directory
//        if (!trackerDirectory.isAbsolute()) {
//            trackerDirectory = new File(spoolDirectory, trackerDirPath);
//        }
//
//        // ensure that meta directory exists
//        if (!trackerDirectory.exists()) {
//            if (!trackerDirectory.mkdir()) {
//                throw new IOException("Unable to mkdir nonexistent meta directory"+trackerDirectory);
//            }
//        }
//
//        // ensure that the meta directory is a directory
//        if (!trackerDirectory.isDirectory()) {
//            throw new IOException("Specified meta directory is not a directory"+ trackerDirectory);
//        }
//
//        this.metaFile = new File(trackerDirectory, metaFileName);
//    }
//
//
//    public String getLastFileRead(){
//        if(!lastFileRead.isPresent()){
//            return null;
//        }
//        return lastFileRead.get().getFile().getAbsolutePath();
//    }
//
//
//    @Override
//    public void commit() throws IOException {
//
//    }
//
//    @Override
//    public Event readEvent() throws IOException {
//        List<Event> events = readEvents(1);
//        if (!events.isEmpty()) {
//            return events.get(0);
//        } else {
//            return null;
//        }
//    }
//
//    @Override
//    public List<Event> readEvents(int i) throws IOException {
//        if (!committed) {
//            if (!currentFile.isPresent()) {
//                throw new IllegalStateException("File should not roll when "
//                        + "commit is outstanding.");
//            }
//            logger.info("Last read was never committed - resetting mark position.");
//            currentFile.get().getDeserializer().reset();
//        } else {
//            // Check if new files have arrived since last call
//            if (!currentFile.isPresent()) {
//                currentFile = getNextFile();
//            }
//            // Return empty list if no new files
//            if (!currentFile.isPresent()) {
//                return Collections.emptyList();
//            }
//        }
//
//        EventDeserializer des = currentFile.get().getDeserializer();
//        List<Event> events = des.readEvents(numEvents);
//
//        /*
//         * It's possible that the last read took us just up to a file boundary.
//         * If so, try to roll to the next file, if there is one.
//         */
//        if (events.isEmpty()) {
//            retireCurrentFile();
//            currentFile = getNextFile();
//            if (!currentFile.isPresent()) {
//                return Collections.emptyList();
//            }
//            events = currentFile.get().getDeserializer().readEvents(numEvents);
//        }
//
//        if (annotateFileName) {
//            String filename = currentFile.get().getFile().getAbsolutePath();
//            for (Event event : events) {
//                event.getHeaders().put(fileNameHeader, filename);
//            }
//        }
//
//        if (annotateBaseName) {
//            String basename = currentFile.get().getFile().getName();
//            for (Event event : events) {
//                event.getHeaders().put(baseNameHeader, basename);
//            }
//        }
//
//        // 增加代码开始
//
//        // 按正则抽取文件名的内容
//        if (annotateFileNameExtractor) {
//
//            Matcher matcher = fileNameExtractorPattern.matcher(currentFile
//                    .get().getFile().getName());
//
//            if (matcher.find()) {
//                String value = matcher.group();
//                if (convertToTimestamp) {
//                    DateTimeFormatter formatter = DateTimeFormat
//                            .forPattern(dateTimeFormat);
//                    DateTime dateTime = formatter.parseDateTime(value);
//
//                    value = Long.toString(dateTime.getMillis());
//                }
//
//                for (Event event : events) {
//                    event.getHeaders().put(fileNameExtractorHeader, value);
//                }
//            }
//
//        }
//
//        // 按分隔符拆分文件名
//        if (splitFileName) {
//            String[] splits = currentFile.get().getFile().getName()
//                    .split(splitBy);
//
//            for (Event event : events) {
//                for (int i = 0; i < splits.length; i++) {
//                    event.getHeaders().put(splitBaseNameHeader + i, splits[i]);
//                }
//
//            }
//
//        }
//
//        // 增加代码结束
//
//        committed = false;
//        lastFileRead = currentFile;
//        return events;
//    }
//
//    @Override
//    public void close() throws IOException {
//
//    }
//}
