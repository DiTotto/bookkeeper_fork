package org.apache.bookkeeper.client;


import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(value = Enclosed.class)
public class LedgerHandleTest {

    @RunWith(Parameterized.class)
    public static class LedgerHandleAsyncReadEntriesTest extends BookKeeperClusterTestCase {

        private LedgerHandle ledgerHandle;
        private final Long firstEntry;
        private final Long lastEntry;
        private final boolean expectException;
        private final Object context;
        private final boolean useCallBack;
        private final int expectErrorCode;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleAsyncReadEntriesTest(Long firstEntry, Long lastEntry, boolean useCallBack,
                                                Object context, boolean expectException, int expectedErrorCode) {
            super(4);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.expectException = expectException;
            this.context = context;
            this.useCallBack = useCallBack;
            this.expectErrorCode = expectedErrorCode;
        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    ///valid
                    {1L, 5L, false, null, false, BKException.Code.OK},
                    {1L, 5L, true, null, false, BKException.Code.OK},
                    {0L, 0L, false, null, false, BKException.Code.OK},
                    //invalid
                    {-1L, 5L, true, new Object(), true, BKException.Code.IncorrectParameterException},
                    {-1L, 5L, false, null, true, BKException.Code.IncorrectParameterException},
                    {5L, -1L, false, null, true, BKException.Code.IncorrectParameterException},
                    {3L, 1L, true, null, true, BKException.Code.IncorrectParameterException},
                    {0L, Long.MAX_VALUE, false, null, true, BKException.Code.ReadException},
                    {Long.MAX_VALUE, Long.MAX_VALUE, false, null, true, BKException.Code.ReadException},
            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            for (int i = 0; i < 7; i++) {
                ledgerHandle.addEntry(("Entry " + i).getBytes());
            }
        }
        @After
        public void tearDownTestEnvironment() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();

            }
        }


        private static BookkeeperInternalCallbacks.WriteCallback mockWriteCallback() {
            return mock(BookkeeperInternalCallbacks.WriteCallback.class);
        }

        // Test principale
        @Test
        public void testAsyncReadEntries() {
            try {
                //ledgerHandle.asyncReadEntries(firstEntry, lastEntry, cb, ctx);

                if (!useCallBack) {
                    ledgerHandle.asyncReadEntries(firstEntry, lastEntry, null, context);

                    if (expectException) {
                        fail("Expected exception, but method executed successfully.");
                    }
                } else {
                    AtomicBoolean isComplete = new AtomicBoolean(false);
                    AtomicInteger resultCode = new AtomicInteger(BKException.Code.OK);

                    ledgerHandle.asyncReadEntries(firstEntry, lastEntry, (rc, lh, entries, ctx) -> {
                        resultCode.set(rc);
                        if (rc == BKException.Code.OK) {
                            assertNotNull("Entries should not be null on successful read", entries);
                            assertTrue("Entries should contain at least one element", entries.hasMoreElements());
                        } else {
                            assertEquals("Unexpected error code", expectErrorCode, rc);
                        }
                        isComplete.set(true);
                    }, context);

                    Awaitility.await().untilTrue(isComplete);

                    if (!expectException) {
                        assertEquals("Expected successful read", BKException.Code.OK, resultCode.get());
                    } else {
                        assertEquals("Expected failure with specific error code", expectErrorCode, resultCode.get());
                    }
                }


            } catch (NullPointerException e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof NullPointerException);
                }
            } catch (Exception e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof BKException);
                }
            }
        }


    }
}
