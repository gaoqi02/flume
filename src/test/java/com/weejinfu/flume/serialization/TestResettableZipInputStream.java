package com.weejinfu.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.weejinfu.flume.utils.ZipUtil;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by Jason on 15/8/17.
 */
public class TestResettableZipInputStream {

    private static final boolean CLEANUP = true;
    private static final File WORK_DIR =
            new File("target/test/work").getAbsoluteFile();
    private static final Logger logger = LoggerFactory.getLogger
            (TestResettableZipInputStream.class);

    private File file, meta, zipFile;

    @Before
    public void setup() throws Exception {
        Files.createParentDirs(new File(WORK_DIR, "dummy"));
        file = File.createTempFile(getClass().getSimpleName(), ".log", WORK_DIR);
        logger.info("Data file: {}", file);
        zipFile = File.createTempFile(getClass().getSimpleName(), ".zip", WORK_DIR);
        logger.info("Zip file: {}", zipFile);
        meta = File.createTempFile(getClass().getSimpleName(), ".txt", WORK_DIR);
        logger.info("PositionTracker meta file: {}", meta);
        meta.delete();
    }

    @After
    public void tearDown() throws Exception {
        if (CLEANUP) {
            meta.delete();
            file.delete();
        }
    }

    @Test
    public void testBasicRead() throws IOException {
        String output = singleLineFileInit(file, Charsets.UTF_8);
        ZipUtil.zipFile(zipFile, file);

        PositionTracker tracker = DurablePositionTracker.getInstance(meta, zipFile.getPath());
        ResettableInputStream in = new ResettableZipInputStream(zipFile, tracker);

        String result = readLine(in, output.length());
        assertEquals(output, result);

        String afterEOF = readLine(in, output.length());
        assertNull(afterEOF);

        in.close();
    }

    private static String singleLineFileInit(File file, Charset charset)
            throws IOException {
        String output = "Weejinfu is gonna be great!\n";
        Files.write(output.getBytes(charset), file);
        return output;
    }

    /**
     * Helper function to read a line from a character stream.
     * @param in
     * @param maxLength
     * @return
     * @throws IOException
     */
    private static String readLine(ResettableInputStream in, int maxLength)
            throws IOException {

        StringBuilder s = new StringBuilder();
        int c;
        int i = 1;
        while ((c = in.readChar()) != -1) {
            // FIXME: support \r\n
            if (c == '\n') {
                break;
            }
            //System.out.printf("seen char val: %c\n", (char)c);
            s.append((char)c);

            if (i++ > maxLength) {
                System.out.println("Output: >" + s + "<");
                throw new RuntimeException("Too far!");
            }
        }
        if (s.length() > 0) {
            s.append('\n');
            return s.toString();
        } else {
            return null;
        }
    }

}
