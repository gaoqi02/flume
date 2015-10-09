package com.weejinfu.flume.client.zip;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.weejinfu.flume.utils.ZipUtil;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableEventReader;
import com.weejinfu.flume.client.zip.ReliableSpoolingZipFileEventReader.DeletePolicy;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;

/**
 * Created by Jason on 15/8/17.
 */
public class TestReliableSpoolingZipFileEventReader {

    private static final Logger logger = LoggerFactory.getLogger
            (TestReliableSpoolingZipFileEventReader.class);

    private static final File WORK_DIR = new File("target/test/work/" +
            TestReliableSpoolingZipFileEventReader.class.getSimpleName());

    @Before
    public void setup() throws IOException, InterruptedException {
        if (!WORK_DIR.isDirectory()) {
            Files.createParentDirs(new File(WORK_DIR, "dummy"));
        }

        // write out a few files
        for (int i = 0; i < 4; i++) {
            File fileName = new File(WORK_DIR, "file" + i);
            StringBuilder sb = new StringBuilder();

            // write as many lines as the index of the file
            for (int j = 0; j < i; j++) {
                sb.append("file" + i + "line" + j + "\n");
            }
            Files.write(sb.toString(), fileName, Charsets.UTF_8);
            ZipUtil.zipFile(new File(WORK_DIR, "file" + i + ".zip"), fileName);
            fileName.delete();
        }
        Thread.sleep(1500L); // make sure timestamp is later
        File emptyFile = new File(WORK_DIR, "emptylineFile");
        Files.write("\n", emptyFile, Charsets.UTF_8);
        ZipUtil.zipFile(new File(WORK_DIR, "emptylineFile.zip"), emptyFile);
        emptyFile.delete();
    }

    @After
    public void tearDown() {
        deleteDir(WORK_DIR);
    }

    private void deleteDir(File dir) {
        // delete all the files & dir we created
        File[] files = dir.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                File[] subDirFiles = f.listFiles();
                for (File sdf : subDirFiles) {
                    if (!sdf.delete()) {
                        logger.warn("Cannot delete file {}", sdf.getAbsolutePath());
                    }
                }
                if (!f.delete()) {
                    logger.warn("Cannot delete directory {}", f.getAbsolutePath());
                }
            } else {
                if (!f.delete()) {
                    logger.warn("Cannot delete file {}", f.getAbsolutePath());
                }
            }
        }
        if (!dir.delete()) {
            logger.warn("Cannot delete work directory {}", dir.getAbsolutePath());
        }
    }

    @Test
    public void testIgnorePattern() throws IOException {
        ReliableEventReader reader = new ReliableSpoolingZipFileEventReader.Builder()
                .spoolDirectory(WORK_DIR)
                .ignorePattern("^file2.zip$")
                .deletePolicy(DeletePolicy.IMMEDIATE.toString())
                .build();

        List<File> before = listFiles(WORK_DIR);
        Assert.assertEquals("Expected 5, not: " + before, 5, before.size());

        List<Event> events;
        do {
            events = reader.readEvents(10);
            reader.commit();
        } while (!events.isEmpty());

        List<File> after = listFiles(WORK_DIR);
        Assert.assertEquals("Expected 1, not: " + after, 1, after.size());
        Assert.assertEquals("file2.zip", after.get(0).getName());
        List<File> trackerFiles = listFiles(new File(WORK_DIR,
                SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR));
        Assert.assertEquals("Expected 0, not: " + trackerFiles, 0,
                trackerFiles.size());
    }

    @Test
    public void testRepeatedCallsWithCommitAlways() throws IOException {
        ReliableEventReader reader = new ReliableSpoolingZipFileEventReader.Builder()
                .spoolDirectory(WORK_DIR).build();

        final int[] expectedLines = {1, 1, 1, 1};
        int seenLines = 0;
        for (int i = 0; i < 4; i++) {
            List<Event> events = reader.readEvents(1);
            seenLines += events.size();
            reader.commit();
            Assert.assertEquals(expectedLines[i], events.size());
        }
    }

    @Test
    public void testRepeatedCallsWithCommitOnSuccess() throws IOException {
        String trackerDirPath =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
        File trackerDir = new File(WORK_DIR, trackerDirPath);

        ReliableEventReader reader = new ReliableSpoolingZipFileEventReader.Builder()
                .spoolDirectory(WORK_DIR).trackerDirPath(trackerDirPath).build();

        final int expectedLines = 1 + 1 + 1 + 1;
        int seenLines = 0;
        for (int i = 0; i < 10; i++) {
            List<Event> events = reader.readEvents(10);
            int numEvents = events.size();
            if (numEvents > 0) {
                seenLines += numEvents;
                reader.commit();

                // ensure that there are files in the trackerDir
                File[] files = trackerDir.listFiles();
                Assert.assertNotNull(files);
                Assert.assertTrue("Expected tracker files in tracker dir " + trackerDir
                        .getAbsolutePath(), files.length > 0);
            }
        }

        Assert.assertEquals(expectedLines, seenLines);
    }

    @Test
    public void testFileDeletion() throws IOException {
        ReliableEventReader reader = new ReliableSpoolingZipFileEventReader.Builder()
                .spoolDirectory(WORK_DIR)
                .deletePolicy(DeletePolicy.IMMEDIATE.name())
                .build();

        List<File> before = listFiles(WORK_DIR);
        Assert.assertEquals("Expected 5, not: " + before, 5, before.size());

        List<Event> events;
        do {
            events = reader.readEvents(10);
            reader.commit();
        } while (!events.isEmpty());

        List<File> after = listFiles(WORK_DIR);
        Assert.assertEquals("Expected 0, not: " + after, 0, after.size());
        List<File> trackerFiles = listFiles(new File(WORK_DIR,
                SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR));
        Assert.assertEquals("Expected 0, not: " + trackerFiles, 0,
                trackerFiles.size());
    }

    @Test(expected = NullPointerException.class)
    public void testNullConsumeOrder() throws IOException {
        new ReliableSpoolingFileEventReader.Builder()
                .spoolDirectory(WORK_DIR)
                .consumeOrder(null)
                .build();
    }

    private static List<File> listFiles(File dir) {
        List<File> files = Lists.newArrayList(dir.listFiles(new FileFilter
                () {
            @Override
            public boolean accept(File pathname) {
                return !pathname.isDirectory();
            }
        }));
        return files;
    }
}
