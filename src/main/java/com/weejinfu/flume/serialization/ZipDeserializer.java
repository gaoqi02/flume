package com.weejinfu.flume.serialization;

import com.google.common.collect.Lists;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;

/**
 * A deserializer that reads a Zip File per event; To be used with
 * Flume SpoolDirectorySource.
 *
 * Created by Jason on 15/8/12.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ZipDeserializer implements EventDeserializer {

    private ResettableInputStream in;
    private final int maxFileLength;
    private volatile boolean isOpen;

    public static final String MAX_FILE_LENGTH_KEY = "maxFileLength";
    public static final int MAX_FILE_LENGTH_DEFAULT = 100 * 1000 * 1000;

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 8;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZipDeserializer.class);

    protected ZipDeserializer(Context context, ResettableInputStream in) {
        this.in = in;
        this.maxFileLength = context.getInteger(MAX_FILE_LENGTH_KEY, MAX_FILE_LENGTH_DEFAULT);
        if (this.maxFileLength <= 0) {
            throw new ConcurrentModificationException("Configuration parameter " + MAX_FILE_LENGTH_KEY
                    + "must be greater than zero: " + maxFileLength);
        }
        this.isOpen = true;
    }

    /**
     * Reads a file and return an event
     * @return event containing a unzip file
     * @throws IOException
     */
    @SuppressWarnings("resource")
    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        ByteArrayOutputStream bos = null;
        byte[] buf = new byte[Math.min(maxFileLength, DEFAULT_BUFFER_SIZE)];
        int fileLength = 0;
        int n = 0;
        while ((n = in.read(buf, 0, Math.min(buf.length, maxFileLength - fileLength))) != -1) {
            if (null == bos) {
                bos = new ByteArrayOutputStream(n);
            }
            bos.write(buf, 0, n);
            fileLength += n;
            if (fileLength >= maxFileLength) {
                LOGGER.warn("File length exceeds maxFileLength ({}), truncating file event!", maxFileLength);
                break;
            }
        }

        if (null == bos) {
            return null;
        } else {
            return EventBuilder.withBody(bos.toByteArray());
        }
    }

    /**
     * Batch file read
     * @param numEvents Maximum number of events to return.
     * @return List of events containing read files
     * @throws IOException
     */
    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        in.mark();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        in.reset();
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            reset();
            in.close();
            isOpen = false;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    /** Builder implementations MUST have a public no-arg constructor */
    public static class Builder implements EventDeserializer.Builder {

        @Override
        public ZipDeserializer build(Context context, ResettableInputStream in) {
            return new ZipDeserializer(context, in);
        }

    }

}
