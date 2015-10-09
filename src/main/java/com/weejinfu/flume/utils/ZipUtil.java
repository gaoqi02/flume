package com.weejinfu.flume.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by Jason on 15/8/14.
 */
public class ZipUtil {

    public static void zipFile(String destFileName, File file) throws IOException {

        zipFile(new File(destFileName), file);

    }

    public static void zipFile(File destFile, File file) throws IOException {

        byte[] buffer = new byte[1024];

        FileOutputStream fos = new FileOutputStream(destFile);
        ZipOutputStream zos = new ZipOutputStream(fos);
        ZipEntry ze = new ZipEntry("file1");
        zos.putNextEntry(ze);
        FileInputStream in = new FileInputStream(file);

        int len;
        while ((len = in.read(buffer)) > 0) {
            zos.write(buffer, 0, len);
        }

        in.close();
        zos.closeEntry();

        zos.close();

    }

}
