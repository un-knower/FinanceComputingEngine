package com.yss.flumesource;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author yupan
 * 2018-08-27 15:04
 */
public class MyFlumeSource  extends AbstractSource implements Configurable, EventDrivenSource {
    private SourceCounter sourceCounter;
    private MySourceEventReader reader;
    private ScheduledExecutorService executor;
    private int intervalMillis;

    @Override
    public void configure(Context context) {
        intervalMillis = context.getInteger("intervalMillis", 100);
    }

    @Override
    public synchronized void start() {
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
        executor = Executors.newSingleThreadScheduledExecutor();
        reader = new MySourceEventReader();

        Runnable runner = new MyReaderRunnable(reader, sourceCounter);
        executor.scheduleWithFixedDelay(runner, 0, 2, TimeUnit.MILLISECONDS);
        super.start();
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.shutdownNow();
        super.stop();
        sourceCounter.stop();
    }


    private class MyReaderRunnable implements Runnable {

        private MySourceEventReader reader;
        private SourceCounter sourceCounter;

        public MyReaderRunnable(MySourceEventReader reader, SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        @Override
        public void run() {
            while(!Thread.interrupted()){
                try {
                    List<Event> events = reader.readEvents(5);
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendAcceptedCount();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}