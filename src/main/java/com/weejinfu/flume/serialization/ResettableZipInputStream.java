package com.weejinfu.flume.serialization;

import com.google.common.base.Charsets;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * For reading zip file, read one zip entry to buffer, so if
 * one entry is bigger than the buf size, will throw exception.
 * This version can only support one entry in a zip file.
 *
 * Created by Jason on 15/8/12.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ResettableZipInputStream extends ResettableInputStream
        implements RemoteMarkable, LengthMeasurable {

    Logger logger = LoggerFactory.getLogger(ResettableZipInputStream.class);

    public static final int DEFAULT_BUF_SIZE = 16384;

    public static final int MAX_ENTRY_SIZE = 65536; // default max zip entry size 64K

    public static final int MAX_ZIP_SIZE = 16384; // default max zip file size 16K

    /**
     * The minimum acceptable buffer size to store bytes read
     * from the underlying file. A minimum size of 8 ensures that the
     * buffer always has enough space to contain multi-byte characters,
     * including special sequences such as surrogate pairs, Byte Order Marks, etc.
     */
    public static final int MIN_BUF_SIZE = 8;

    private final File file;
    private final PositionTracker tracker;
    private ZipInputStream in;
    private ReadableByteChannel chan;
    private final ByteBuffer buf;
    private final CharBuffer charBuf;
    private final byte[] byteBuf;
    private final long fileSize;
    private final CharsetDecoder decoder;
    private long position;
    private long syncPosition;
    private int maxCharWidth;


    /**
     * Whether this instance holds a low surrogate character.
     */
    private boolean hasLowSurrogate = false;

    /**
     * A low surrrgate character read from a surrogate pair.
     * When a surrogate pair is found, the high (first) surrogate pair
     * is returned upon a call to {@link #read()},
     * while the low (second) surrogate remains stored in memory,
     * to be returned at the next call to {@link #read()}.
     */
    private char lowSurrogate;

    /**
     *
     * @param file
     *        File to read
     *
     * @param tracker
     *        PositionTracker implementation to make offset position durable
     *
     * @throws java.io.FileNotFoundException If the file to read does not exist
     * @throws java.io.IOException If the position reported by the tracker cannot be sought
     */
    public ResettableZipInputStream(File file, PositionTracker tracker)
            throws IOException {
        this(file, tracker, DEFAULT_BUF_SIZE, Charsets.UTF_8, DecodeErrorPolicy.FAIL);
    }

    /**
     *
     * @param file
     *        File to read
     *
     * @param tracker
     *        PositionTracker implementation to make offset position durable
     *
     * @param bufSize
     *        Size of the underlying buffer used for input. If lesser than {@link #MIN_BUF_SIZE},
     *        a buffer of length {@link #MIN_BUF_SIZE} will be created instead.
     *
     * @param charset
     *        Character set used for decoding text, as necessary
     *
     * @param decodeErrorPolicy
     *        A {@link DecodeErrorPolicy} instance to determine how
     *        the decoder should behave in case of malformed input and/or
     *        unmappable character.
     *
     * @throws java.io.FileNotFoundException If the file to read does not exist
     * @throws IOException If the position reported by the tracker cannot be sought
     */
    public ResettableZipInputStream(File file, PositionTracker tracker,
                                     int bufSize, Charset charset, DecodeErrorPolicy decodeErrorPolicy)
            throws IOException {
        this.file = file;
        this.fileSize = file.length();
        this.tracker = tracker;
        this.in = new ZipInputStream(new FileInputStream(file));
        this.buf = ByteBuffer.allocateDirect(Math.max(Math.max(bufSize, MIN_BUF_SIZE), MAX_ENTRY_SIZE));
        buf.flip();
        this.byteBuf = new byte[1]; // single byte
        this.charBuf = CharBuffer.allocate(2); // two chars for surrogate pairs
        charBuf.flip();
        this.decoder = charset.newDecoder();
        this.position = 0;
        this.syncPosition = 0;

        // get entry in zip file
        ZipEntry zipEntry = in.getNextEntry();
        if (null == zipEntry) {
            // there no file in the zip file
            throw new IOException("There is no compressed file in the zip file.");
        }

        // check zip size
        if (fileSize > MAX_ZIP_SIZE) {
            throw new IndexOutOfBoundsException("Zip size is out of file size limit "
                    + MAX_ZIP_SIZE);
        }

        this.chan = Channels.newChannel(in);

        if(charset.name().startsWith("UTF-8")) {
            // some JDKs wrongly report 3 bytes max
            this.maxCharWidth = 4;
        } else if(charset.name().startsWith("UTF-16")) {
            // UTF_16BE and UTF_16LE wrongly report 2 bytes max
            this.maxCharWidth = 4;
        } else if(charset.name().startsWith("UTF-32")) {
            // UTF_32BE and UTF_32LE wrongly report 4 bytes max
            this.maxCharWidth = 8;
        } else {
            this.maxCharWidth = (int) Math.ceil(charset.newEncoder().maxBytesPerChar());
        }

        CodingErrorAction errorAction;
        switch (decodeErrorPolicy) {
            case FAIL:
                errorAction = CodingErrorAction.REPORT;
                break;
            case REPLACE:
                errorAction = CodingErrorAction.REPLACE;
                break;
            case IGNORE:
                errorAction = CodingErrorAction.IGNORE;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unexpected value for decode error policy: " + decodeErrorPolicy);
        }
        decoder.onMalformedInput(errorAction);
        decoder.onUnmappableCharacter(errorAction);

        refillBuf();
        seek(tracker.getPosition());
    }

    @Override
    public synchronized int read() throws IOException {
        int len = read(byteBuf, 0, 1);
        if (len == -1) {
            return -1;
            // len == 0 should never happen
        } else if (len == 0) {
            return -1;
        } else {
            return byteBuf[0];
        }
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        logger.trace("read(buf, {}, {})", off, len);

        if (!buf.hasRemaining()) {
            return -1;
        }

        int rem = buf.remaining();
        if (len > rem) {
            len = rem;
        }
        buf.get(b, off, len);
        incrPosition(len, true);
        return len;
    }

    @Override
    public synchronized int readChar() throws IOException {
        // TODO: need to rewrite and test this method

        // Check whether we are in the middle of a surrogate pair,
        // in which case, return the last (low surrogate) char of the pair.
        if(hasLowSurrogate) {
            hasLowSurrogate = false;
            return lowSurrogate;
        }

        // The decoder can have issues with multi-byte characters.
        // This check ensures that there are at least maxCharWidth bytes in the buffer
        // before reaching EOF.
//        if (buf.remaining() < maxCharWidth) {
//            buf.clear();
//            buf.flip();
//            refillBuf();
//        }

        int start = buf.position();
        charBuf.clear();
        charBuf.limit(1);

        boolean isEndOfInput = false;
        if (!buf.hasRemaining()) {
            isEndOfInput = true;
        }

        CoderResult res = decoder.decode(buf, charBuf, isEndOfInput);
        if (res.isMalformed() || res.isUnmappable()) {
            res.throwException();
        }

        int delta = buf.position() - start;

        charBuf.flip();

        // Found a single char
        if (charBuf.hasRemaining()) {
            char c = charBuf.get();
            incrPosition(delta, true);
            return c;
        }

        // Found nothing, but the byte buffer has not been entirely consumed.
        // This situation denotes the presence of a surrogate pair
        // that can only be decoded if we have a 2-char buffer.
        if(buf.hasRemaining()) {
            charBuf.clear();
            // increase the limit to 2
            charBuf.limit(2);
            // decode 2 chars in one pass
            res = decoder.decode(buf, charBuf, isEndOfInput);
            if (res.isMalformed() || res.isUnmappable()) {
                res.throwException();
            }
            charBuf.flip();
            // Check if we successfully decoded 2 chars
            if (charBuf.remaining() == 2) {
                char highSurrogate = charBuf.get();
                // save second (low surrogate) char for later consumption
                lowSurrogate = charBuf.get();
                // Check if we really have a surrogate pair
                if( ! Character.isHighSurrogate(highSurrogate) || ! Character.isLowSurrogate(lowSurrogate)) {
                    // This should only happen in case of bad sequences (dangling surrogate, etc.)
                    logger.warn("Decoded a pair of chars, but it does not seem to be a surrogate pair: {} {}", (int)highSurrogate, (int)lowSurrogate);
                }
                hasLowSurrogate = true;
                // consider the pair as a single unit and increment position normally
                delta = buf.position() - start;
                incrPosition(delta, true);
                // return the first (high surrogate) char of the pair
                return highSurrogate;
            }
        }

        // end of file
        incrPosition(delta, false);
        return -1;

    }


    // load the whole entry to the buf
    private void refillBuf() throws IOException {
        buf.clear();
        chan.read(buf);
        buf.flip();
    }

    @Override
    public void mark() throws IOException {
        tracker.storePosition(tell());
    }

    @Override
    public void markPosition(long position) throws IOException {
        tracker.storePosition(position);
    }

    @Override
    public long getMarkPosition() throws IOException {
        return tracker.getPosition();
    }

    @Override
    public void reset() throws IOException {
        this.in = new ZipInputStream(new FileInputStream(file));
        in.getNextEntry();
        this.chan = Channels.newChannel(in);
        refillBuf();
        seek(tracker.getPosition());
    }

    @Override
    public long length() throws IOException {
        return file.length();
    }

    @Override
    public long tell() throws IOException {
        logger.trace("Tell position: {}", syncPosition);

        return syncPosition;
    }

    @Override
    public synchronized void seek(long newPos) throws IOException {
        logger.trace("Seek to position: {}", newPos);

        // check to see if we can seek within our existing buffer
        long relativeChange = newPos - position;
        if (relativeChange == 0) return; // seek to current pos => no-op

        int skipBytes = (int)newPos;
        byte[] bytes = new byte[skipBytes];
        buf.get(bytes, 0, skipBytes);

        // clear decoder state
        decoder.reset();

        // reset position pointers
        position = syncPosition = newPos;
    }

    private void incrPosition(int incr, boolean updateSyncPosition) {
        position += incr;
        if (updateSyncPosition) {
            syncPosition = position;
        }
    }

    @Override
    public void close() throws IOException {
        tracker.close();
        in.close();
    }

}
