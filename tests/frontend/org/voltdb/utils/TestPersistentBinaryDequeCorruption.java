/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.voltdb.utils.TestPersistentBinaryDeque.defaultBuffer;
import static org.voltdb.utils.TestPersistentBinaryDeque.defaultContainer;
import static org.voltdb.utils.TestPersistentBinaryDeque.pollOnceAndVerify;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.NavigableMap;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.test.utils.RandomTestRule;

import com.google_voltpatches.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class TestPersistentBinaryDequeCorruption {
    private static final VoltLogger LOG = new VoltLogger("TEST");
    private static final String TEST_NONCE = "pbd_nonce";
    private static final String CURSOR_ID = "TestPersistentBinaryDequeCorruption";

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    @Rule
    public final RandomTestRule random = new RandomTestRule();

    private final CorruptionChecker m_checker;
    private PersistentBinaryDeque m_pbd;
    private DeferredSerialization m_ds;

    @Parameters
    public static Collection<Object[]> parameters() {
        CorruptionChecker scanEntries = pbd -> pbd.scanEntries(b -> {});
        CorruptionChecker parseAndTruncate = pbd -> pbd.parseAndTruncate(b -> null);
        return ImmutableList.of(new Object[] { scanEntries }, new Object[] { parseAndTruncate });
    }

    public TestPersistentBinaryDequeCorruption(CorruptionChecker checker) {
        m_checker = checker;
    }

    @Before
    public void setup() throws IOException {
        m_ds = new DeferredSerialization() {
            private final ByteBuffer data = ByteBuffer.allocate(245);

            {
                random.nextBytes(data.array());
            }

            @Override
            public void serialize(ByteBuffer buf) throws IOException {
                data.rewind();
                buf.put(data);
            }

            @Override
            public int getSerializedSize() throws IOException {
                return data.limit();
            }

            @Override
            public void cancel() {}
        };
        m_pbd = newPbd();
    }

    @After
    public void tearDown() throws IOException {
        m_pbd.close();
    }

    @Test
    public void testCorruptedEntry() throws Exception {
        m_pbd.offer(defaultContainer());
        corruptLastSegment(ByteBuffer.allocateDirect(35), -35);

        runCheckerNewPbd();
    }

    @Test
    public void testCorruptedEntryLength() throws Exception {
        // set no extraHeader so it is easier to find the first entry header
        m_pbd.updateExtraHeader(null);

        BBContainer container = defaultContainer();
        int origLength = container.b().remaining();
        m_pbd.offer(container);

        ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
        bb.putInt(origLength - 100);
        bb.flip();
        corruptLastSegment(bb, PBDSegment.SEGMENT_HEADER_BYTES + PBDSegment.ENTRY_HEADER_TOTAL_BYTES_OFFSET);

        runCheckerNewPbd();
    }

    @Test
    public void testCorruptSegmentHeader() throws Exception {
        m_pbd.offer(defaultContainer());
        ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
        bb.putInt(100);
        bb.flip();
        corruptLastSegment(bb, PBDSegment.HEADER_NUM_OF_ENTRY_OFFSET);

        runCheckerNewPbd();
    }

    @Test
    public void testCorruptExtraHeader() throws Exception {
        m_pbd.offer(defaultContainer());
        ByteBuffer bb = ByteBuffer.allocateDirect(40);
        corruptLastSegment(bb, PBDSegment.HEADER_EXTRA_HEADER_OFFSET + 15);

        runCheckerNewPbd();
    }

    @Test
    public void testCloseLastReader() throws Exception {
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        pollOnceAndVerify(reader, null);
        m_pbd.closeCursor(CURSOR_ID);
        m_pbd.offer(defaultContainer());
    }

    @Test
    public void testQuarantinedFileDeletedWhenPassed() throws Exception {
        ByteBuffer data = defaultBuffer();
        for (int i = 0; i < 5; ++i) {
            for (int j = 0; j < 5; ++j) {
                m_pbd.offer(defaultContainer());
            }
            m_pbd.updateExtraHeader(m_ds);
        }
        assertEquals(6, testDir.getRoot().list().length);

        int i = 0;
        for (PBDSegment segment : getSegmentMap().values()) {
            switch (i++) {
            case 1:
                corruptSegment(segment, ByteBuffer.allocateDirect(10), PBDSegment.HEADER_NUM_OF_ENTRY_OFFSET);
                break;
            case 3:
                corruptSegment(segment, ByteBuffer.allocateDirect(10), PBDSegment.HEADER_EXTRA_HEADER_OFFSET + 20);
                break;
            }
        }

        int quarantineCount = 0;
        String[] entries = testDir.getRoot().list();
        assertEquals(6, testDir.getRoot().list().length);
        for (String entry : entries) {
            if (PbdSegmentName.parseName(null, entry).m_quarantined) {
                ++quarantineCount;
            }
        }

        assertEquals(0, quarantineCount);

        m_checker.run(m_pbd);
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        BinaryDequeReader reader2 = m_pbd.openForRead(CURSOR_ID + 2);

        quarantineCount = 0;
        entries = testDir.getRoot().list();
        assertEquals(6, testDir.getRoot().list().length);
        for (String entry : entries) {
            if (PbdSegmentName.parseName(null, entry).m_quarantined) {
                ++quarantineCount;
            }
        }

        assertEquals(2, quarantineCount);

        for (i = 0; i < 15; ++i) {
            pollOnceAndVerify(reader2, data);
        }
        pollOnceAndVerify(reader2, null);

        for (i = 0; i < 3; ++i) {
            for (int j = 0; j < 5; ++j) {
                pollOnceAndVerify(reader, data);
                if (j == 0) {
                    assertEquals(6 - (i * 2), testDir.getRoot().list().length);
                }
            }
        }
        pollOnceAndVerify(reader, null);

        assertEquals(1, testDir.getRoot().list().length);
    }

    private void runCheckerNewPbd() throws IOException {
        PersistentBinaryDeque pbd = newPbd();
        try {
            m_checker.run(pbd);
            pollOnceAndVerify(pbd.openForRead(CURSOR_ID), null);
            pbd.offer(defaultContainer());
            pollOnceAndVerify(pbd.openForRead(CURSOR_ID), defaultBuffer());
        } finally {
            pbd.close();
        }
    }

    private void corruptLastSegment(ByteBuffer corruptData, int position) throws Exception {
        corruptSegment(getSegmentMap().lastEntry().getValue(), corruptData, position);
    }

    private static void corruptSegment(PBDSegment segment, ByteBuffer corruptData, int position) throws IOException {
        File file = segment.file();
        try (FileChannel channel = FileChannel.open(Paths.get(file.getPath()), StandardOpenOption.WRITE)) {
            channel.write(corruptData, position < 0 ? channel.size() + position : position);
        }
    }

    @SuppressWarnings("unchecked")
    private NavigableMap<Long, PBDSegment> getSegmentMap() throws IllegalArgumentException, IllegalAccessException {
        return ((NavigableMap<Long, PBDSegment>) FieldUtils
                .getDeclaredField(PersistentBinaryDeque.class, "m_segments", true).get(m_pbd));
    }

    private PersistentBinaryDeque newPbd() throws IOException {
        return new PersistentBinaryDeque(TEST_NONCE, m_ds, testDir.getRoot(), LOG);
    }

    private interface CorruptionChecker {
        void run(PersistentBinaryDeque pbd) throws IOException;
    }
}
