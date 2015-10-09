package com.weejinfu.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.weejinfu.flume.utils.ZipUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Jason on 15/8/14.
 */
public class TestZipDeserializer extends Assert {

//    private ResettableZipInputStream in;
    private File tmpDir;
    private File file;
    private String zipFile;
    private String metaFileName;
    private String content;

    @Before
    public void setup() {
        tmpDir = Files.createTempDir();
        file = new File(tmpDir.getAbsolutePath() + "/file1");
        zipFile = tmpDir.getAbsolutePath() + "/file1.zip";
        metaFileName = ".flumespool-main.meta";
        content = "file1line1\nfile1line2\nfile1line3\n";
    }

    @After
    public void tearDown() {
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();
    }

    @Test
    public void testSimple() throws IOException {
        ResettableInputStream in = setTestInputStream();
        EventDeserializer des = new ZipDeserializer(new Context(), in);
        validateZipParse(des);
    }

    @Test
    public void testSimpleViaBuilder() throws IOException {
        ResettableInputStream in = setTestInputStream();
        EventDeserializer.Builder builder = new ZipDeserializer.Builder();
        EventDeserializer des = builder.build(new Context(), in);
        validateZipParse(des);
    }

    @Test
    public void testSimpleViaFactory() throws IOException {
        ResettableInputStream in = setTestInputStream();
        EventDeserializer des = EventDeserializerFactory.getInstance(ZipDeserializer.Builder.class.getName(),
                new Context(), in);
        validateZipParse(des);
    }

    @Test
    public void testBatch() throws IOException {
        ResettableInputStream in = setTestInputStream();
        EventDeserializer des = new ZipDeserializer(new Context(), in);
        List<Event> events = des.readEvents(10);
        assertEquals(1, events.size());
        assertEventBodyEquals(content, events.get(0));

        des.mark();
        des.close();
    }

    @Test
    public void testMaxFileLength() throws IOException {
        ResettableInputStream in = setTestInputStream();
        Context ctx = new Context();
        ctx.put(ZipDeserializer.MAX_FILE_LENGTH_KEY, "10");

        EventDeserializer des = new ZipDeserializer(ctx, in);
        assertEventBodyEquals("file1line1", des.readEvent());
        assertEventBodyEquals("\nfile1line", des.readEvent());
        assertEventBodyEquals("2\nfile1lin", des.readEvent());
        assertEventBodyEquals("e3\n", des.readEvent());
        assertNull(des.readEvent());
    }

    private void assertEventBodyEquals(String expected, Event event) {
        String bodyStr = new String(event.getBody(), Charsets.UTF_8);
        assertEquals(expected, bodyStr);
    }

    private void validateZipParse(EventDeserializer des) throws IOException {
        Event evt;

        des.mark();
        evt = des.readEvent();
        assertEquals(new String(evt.getBody()), content);
        des.reset();

        evt = des.readEvent();
        assertEquals("data should be repeated, " +
                "because we reset() the stream", new String(evt.getBody()), content);

        evt = des.readEvent();
        assertNull("Event should be null because there are no lines " +
                "left to read", evt);

        des.mark();
        des.close();
    }

    private ResettableZipInputStream setTestInputStream() {
        try {
            Files.write(content, file, Charsets.UTF_8);
        } catch (IOException e) {
            assertTrue("Fail to write a file!", false);
            return null;
        }

        try {
            ZipUtil.zipFile(zipFile, file);
            if (!file.delete()) {
                assertTrue("Fail to delete the original file!", false);
            }
        } catch (IOException e) {
            assertTrue("Fail to zip a file!", false);
            return null;
        }

        try {
            File metaFile = new File(tmpDir, metaFileName);
            PositionTracker tracker = DurablePositionTracker.getInstance(metaFile, zipFile);
            return new ResettableZipInputStream(new File(zipFile), tracker);
        } catch (IOException e) {
            assertTrue("Fail to create ResettableZipInputStream", false);
            return null;
        }
    }
}
