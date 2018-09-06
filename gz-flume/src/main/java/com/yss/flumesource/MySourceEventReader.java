package com.yss.flumesource;

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.event.EventBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author yupan
 * 2018-08-27 14:55
 */
public class MySourceEventReader implements ReliableEventReader {

    private final Charset outputCharset=Charset.forName("UTF-8");
    private boolean committed = true;
    @Override
    public void commit() throws IOException {
        if (!committed) {
            committed = true;
        }
    }

    @Override
    public Event readEvent() throws IOException {
        return EventBuilder.withBody("test", outputCharset);
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        List<Event> retList = Lists.newLinkedList();
        for (int i = 0; i < numEvents; ++i) {
            retList.add(EventBuilder.withBody("test", outputCharset));
        }
        if (retList.size() != 0) {
            // only non-empty events need to commit
            committed = false;
        }
        return retList;
    }

    @Override
    public void close() throws IOException {

    }
}
