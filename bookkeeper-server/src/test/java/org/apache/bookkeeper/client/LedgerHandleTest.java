package org.apache.bookkeeper.client;


import io.reactivex.rxjava3.core.Completable;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(value = Enclosed.class)
public class LedgerHandleTest {
//    @Ignore
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
//                    {1L, 5L, false, null, false, BKException.Code.OK},
//                    {1L, 5L, true, null, false, BKException.Code.OK},
//                    {0L, 0L, false, null, false, BKException.Code.OK},
//                    //invalid
//                    {-1L, 5L, true, new Object(), true, BKException.Code.IncorrectParameterException},
//                    {-1L, 5L, false, null, true, BKException.Code.IncorrectParameterException},
//                    {5L, -1L, false, null, true, BKException.Code.IncorrectParameterException},
//                    {3L, 1L, true, null, true, BKException.Code.IncorrectParameterException},
//                    {0L, Long.MAX_VALUE, false, null, true, BKException.Code.ReadException},
//                    {Long.MAX_VALUE, Long.MAX_VALUE, false, null, true, BKException.Code.ReadException},

                    //test aggiunto per badua
                    {8L, 10L, true, null, true, BKException.Code.ReadException},
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
    } //controllato


    @RunWith(Parameterized.class)
    public static class LedgerHandleAsyncReadLastEntryTest extends BookKeeperClusterTestCase {

        LedgerHandle ledgerHandle;
        private final boolean expectException;
        private final Object context;
        private final int numOfEntry;
        private final int expectErrorCode;
        private final AsyncCallback.ReadCallback readCallback;
        private final boolean useCallBack;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleAsyncReadLastEntryTest(boolean useCallback, int numOfEntry, AsyncCallback.ReadCallback readCallback,
                                                  Object context, boolean expectException, int expectedErrorCode) {
            super(4);
            this.expectException = expectException;
            this.context = context;
            this.numOfEntry = numOfEntry;
            this.expectErrorCode = expectedErrorCode;
            this.readCallback = readCallback;
            this.useCallBack = useCallback;
        }

        private static AsyncCallback.ReadCallback mockReadCallback() {
            return (rc, ledgerHandle, entries, ctx) -> {
                if (rc == BKException.Code.OK) {
                    // Operazione riuscita: entries dovrebbe contenere dati validi
                    assertNotNull("Entries should not be null on successful read", entries);
                    assertTrue("Entries should contain at least one element", entries.hasMoreElements());
                    System.out.println("ReadCallback succeeded: entries fetched.");
                } else {
                    // Operazione fallita: stampa un messaggio con il codice di errore
                    System.out.println("ReadCallback failed with result code: " + rc);
                    assertNull("Entries should be null on failure", entries);
                }
            };
        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    ///valid
                    {true, 5, mockReadCallback(), new Object(), false, BKException.Code.OK},
                    {true, 1, mockReadCallback(), new Object(), false, BKException.Code.OK},
                    {false, 5, mockReadCallback(), new Object(), false, BKException.Code.OK},
                    //invalid
                    {true, 0, mockReadCallback(), new Object(), true, BKException.Code.NoSuchEntryException},
                    {true, -1, mockReadCallback(), new Object(), true, BKException.Code.NoSuchEntryException},
                    {false, 0, mockReadCallback(), new Object(), true, BKException.Code.NoSuchEntryException},
            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());

            if(numOfEntry == 0){
                return;
            }else{
                for (int i = 0; i < numOfEntry; i++) {
                    ledgerHandle.addEntry(("Entry " + i).getBytes());
                }
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



        // Test principale
        @Test
        public void testAsyncReadLastEntry() {
            try {
                if(useCallBack) {
                    //ledgerHandle.asyncReadLastEntry(readCallback, context);
                    AtomicInteger resultCode = new AtomicInteger();
                    AtomicBoolean isComplete = new AtomicBoolean(false);
                    ledgerHandle.asyncReadLastEntry((rc, lh, entries, ctx) -> {
                        resultCode.set(rc);

                        if (rc == BKException.Code.OK) {
                            assertNotNull("Entries should not be null on successful read", entries);

                            //pit
                            assertTrue("Last entry should be <= lastAddConfirmed", entries.nextElement().entryId <= ledgerHandle.getLastAddConfirmed());
                        } else {
                            assertEquals("Unexpected error code", expectErrorCode, rc);
                        }
                        isComplete.set(true);
                    }, context);
                    if (!expectException) {
                        assertEquals("Expected successful read", BKException.Code.OK, resultCode.get());
                    } else {
                        assertEquals("Expected failure with specific error code", expectErrorCode, resultCode.get());

                    }
                }else{
                    ledgerHandle.asyncReadLastEntry(null, context);
                    if (expectException) {
                        fail("Expected exception, but method executed successfully.");
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
    } // 100% coverage

//    @Ignore
    @RunWith(Parameterized.class)
    public static class LedgerHandleAsyncReadLastConfirmedAndEntryTest extends BookKeeperClusterTestCase {

        LedgerHandle ledgerHandle;
        private final boolean expectException;
        private Long entryId;
        private final boolean parallel;
        private final Long timeOutInMillis;
        private final Object context;
        private final int numOfEntry;
        private final int expectErrorCode;
        private final boolean useCallBack;
        private final boolean close;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleAsyncReadLastConfirmedAndEntryTest(Long entryId, boolean close, boolean parallel, Long timeOutInMillis, boolean useCallback, int numOfEntry,
                                                              Object context, boolean expectException, int expectedErrorCode) {
            super(4);
            this.entryId = entryId;
            this.parallel = parallel;
            this.timeOutInMillis = timeOutInMillis;
            this.expectException = expectException;
            this.context = context;
            this.numOfEntry = numOfEntry;
            this.expectErrorCode = expectedErrorCode;
            this.close = close;

            this.useCallBack = useCallback;
        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // {entryId, close, parallel, timeOutInMillis, useCallback, numOfEntry, context, expectException, expectedErrorCode}
                    ///valid
                    {1L, true, false, 100L, true, 5, new Object(), false, BKException.Code.OK}, //callBack
                    {1L, false, true, 100L, false, 5, new Object(), false, BKException.Code.OK}, //no callBack
                    {1L, true, false, 100L, true, 5, new Object(), false, BKException.Code.OK}, //callBack, no parallelism
                    {0L, true, true, 100L, true, 5, new Object(), false, BKException.Code.OK}, //entryId = 0
                    {0L, true, true, 100L, true, 0, new Object(), false, BKException.Code.OK}, //numOfEntry = 0
                    ///invalid
                    {5L, true, true, 100L, true, 3, new Object(), true, BKException.Code.OK}, //entryId = 0
                    {-1L, true, true, 100L, true, 3, new Object(), true, BKException.Code.NoSuchEntryException}, //no callBack
                    {Long.MAX_VALUE, false, true, 100L, true, 5, new Object(), true, BKException.Code.NoSuchEntryException}, //entryId = Long.MAX_VALUE
                    {1L, false, true, 100L, true, 0, new Object(), true, BKException.Code.NoSuchEntryException}, //numOfEntry = 0

            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());

            if(numOfEntry == 0){
                return;
            }else{
                for (int i = 0; i < numOfEntry; i++) {
                    ledgerHandle.addEntry(("Entry " + i).getBytes()); //entryId assume l'id della ultima entry inserita

                }
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



        // Test principale
        @Test
        public void testAsyncReadLastConfirmedAndEntry() {
            try {
                if(useCallBack) {

                    AtomicInteger resultCode = new AtomicInteger();
                    AtomicBoolean isComplete = new AtomicBoolean(false);
                    AtomicLong rLAC = new AtomicLong();
                    AtomicReference<LedgerEntry> rEntry = new AtomicReference<>();
                    if(close){
                        ledgerHandle.close();
                    }
                    long lastEntryId = ledgerHandle.getLastAddConfirmed();
                    ledgerHandle.asyncReadLastConfirmedAndEntry(this.entryId, this.timeOutInMillis, this.parallel, (rc, lastConfirmed, entry, ctx) -> {
                        resultCode.set(rc);
                        rEntry.set(entry);
                        rLAC.set(lastConfirmed);

                        isComplete.set(true);

                        if (resultCode.get() == BKException.Code.OK) {
                            if(!expectException){
                                if(entryId == 0 && numOfEntry == 0){
                                    assertEquals("Expected successful read", BKException.Code.OK, resultCode.get());
                                    assertNull(rEntry.get());
                                    assertEquals(-1, rLAC.get());
                                }else if(entryId != 0 && numOfEntry != 0) {
                                    assertNotNull("Entries should not be null on successful read", rEntry.get());
                                }
                            }
                            else{
                                assertNull("Entries should be null on failure", rEntry.get());
                            }

                            if(entryId > lastEntryId){
                                //assertEquals("Expected failure with specific error code", BKException.Code.NoSuchEntryException, rc);
                                assertNull(rEntry.get());
                                assertEquals(lastEntryId, rLAC.get());
                            }else if (entryId == lastEntryId){
                                assertEquals("Expected successful read", BKException.Code.OK, resultCode.get());
                                assertNotNull(rEntry.get());
                                assertEquals(lastEntryId, rLAC.get());
                                assertEquals("Entry", new String(rEntry.get().getEntry()));

                            }else {
                                assertNull(rEntry.get());
                            }
                        } else {
                            assertNull(rEntry.get());

                        }


                    }, context);

                    //Awaitility.await().untilTrue(isComplete);


                }else{
                    if(close){
                        ledgerHandle.close();
                    }
                    ledgerHandle.asyncReadLastConfirmedAndEntry(this.entryId, this.timeOutInMillis, this.parallel, null, context);
                    if (expectException) {
                        fail("Expected exception, but method executed successfully.");
                    }
                }

            }catch (ArrayIndexOutOfBoundsException e){
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof ArrayIndexOutOfBoundsException);
                }
            }
            catch (NullPointerException e) {
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
    } //aumentata coverage con jacoco
//    @Ignore
    @RunWith(Parameterized.class)
    public static class LedgerHandleAsyncReadTest extends BookKeeperClusterTestCase {

        private LedgerHandle ledgerHandle;
        private final Long firstEntry;
        private final Long lastEntry;
        private final boolean expectException;
        private final Long numEntries;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleAsyncReadTest(Long numEntries, Long firstEntry, Long lastEntry,
                                                 boolean expectException) {
            super(4);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.expectException = expectException;
                        this.numEntries = numEntries;
        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    ///valid
                    {7L, 1L, 5L,   false},
                    //{1L, 5L, true,  false},
//                    {0L, 0L, false,  false, BKException.Code.OK},
//                    //invalid
                    {7L, 1L, 12L,   true},
                    {7L, 6L, 12L,   true},
                    {7L, -1L, 12L,   true},
                    {7L, 1L, -5L,   true},
                    {7L, 8L, 5L,   true},
                    {7L, -1L, Long.MAX_VALUE,   true},
            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            for (int i = 0; i < numEntries; i++) {
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


                CompletableFuture<LedgerEntries> entries = ledgerHandle.readAsync(firstEntry, lastEntry);
                try {
                    entries.whenComplete((ledgerEntries, throwable) -> {
                        if (throwable != null) {

                            //pit
                            if (lastEntry <= ledgerHandle.lastAddConfirmed) {
                                fail("Unexpected exception: lastEntry is less than or equal to lastAddConfirmed, but got an error: " + throwable.getMessage());
                            }
                            if (expectException) {
                                assert (throwable instanceof BKException);
                                //assertTrue("Expected exception, but got: " + throwable.getClass().getSimpleName(),
                                //throwable instanceof BKException);
                            } else {
                                fail("Did not expect an exception, but got: " + throwable.getMessage());
                            }
                        } else {
                            assertNotNull("Entries should not be null on successful read", ledgerEntries);
                            assertNotNull("Entries should contain at least one element", entries);
                            assertNotNull("Entries should not be null on successful read", entries.join());
                            //assertTrue("Entries should contain at least one element", entries.join());
                            long countEntries = 0L;
                            LedgerEntries ledgerEntries1 = entries.join();
                            for (org.apache.bookkeeper.client.api.LedgerEntry entry : ledgerEntries1) {
                                countEntries++;
                                System.out.println("EntryId: " + entry.getEntryId());
                            }
                            assertEquals("Entries should contain exactly " + (lastEntry - firstEntry + 1) + " elements",
                                    (lastEntry - firstEntry + 1), countEntries);
                        }
                    }).join(); //pit, join() per aspettare la fine del completamento altrimenti non veniva catturata la fail
                } catch (CompletionException e) {
                    if (!expectException) {
                        fail("Did not expect an exception, but got: " + e.getMessage());
                    } else {
                        assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                                e instanceof CompletionException);
                    }
                }

            } catch (CompletionException e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof CompletionException);
                }
            }
            catch (NullPointerException e) {
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
    } //100% coverage
//    @Ignore
    @RunWith(Parameterized.class)
    public static class LedgerHandleReadLastConfirmedTest extends BookKeeperClusterTestCase {

        LedgerHandle ledgerHandle;
        private final boolean expectException;
        private Long entryId;
        private final int numOfEntry;
        private final int expectErrorCode;


        // Costruttore per inizializzare i parametri del test
        public LedgerHandleReadLastConfirmedTest(int numOfEntry,  boolean expectException, int expectedErrorCode) {
            super(4);
            this.entryId = entryId;
            this.expectException = expectException;
            this.expectErrorCode = expectedErrorCode;
            this.numOfEntry = numOfEntry;

        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // {entryId, parallel, timeOutInMillis, useCallback, numOfEntry, context, expectException, expectedErrorCode}
                    ///valid
                    {8, false, BKException.Code.OK},
                    /// invalid
                    {0, true, BKException.Code.NoSuchEntryException},
                    {-1, true, BKException.Code.NoSuchEntryException},
//
            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());

            if(numOfEntry <= 0){
                return;
            }else{
                for (int i = 0; i < numOfEntry; i++) {
                    ledgerHandle.addEntry(("Entry " + i).getBytes()); //entryId assume l'id della ultima entry inserita
                }
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



        // Test principale
        @Test
        public void testReadLastConfirmed() {
            try {

                long lastConfirmed = ledgerHandle.readLastConfirmed();
                if( lastConfirmed >= 0) {
                    if (!expectException) {
                        //assertEquals("Expected successful read", BKException.Code.OK, BKException.Code.OK);
                        assert(lastConfirmed >= 0);
                    } else {
                        fail("Did not expect an exception, but got: " + lastConfirmed);
                    }
                }
                else{
                    if (!expectException) {
                        fail("Did not expect an exception, but got: " + lastConfirmed);
                    } else {
                        assertEquals(-1, lastConfirmed);
                    }
                }

            }catch (ArrayIndexOutOfBoundsException e){
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof ArrayIndexOutOfBoundsException);
                }
            }
            catch (NullPointerException e) {
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
    } //forse si puo aumentare coverage
//    @Ignore
    @RunWith(Parameterized.class)
    public static class LedgerHandleAsyncAddEntryTest extends BookKeeperClusterTestCase {

        LedgerHandle ledgerHandle;
        private final boolean expectException;
        private Long entryId;
        private final Object context;
        private final int numOfEntry;
        private final int expectErrorCode;
        private final boolean useCallBack;
        private final byte[] data;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleAsyncAddEntryTest(byte[] data, boolean useCallback, int numOfEntry,
                                                              Object context, boolean expectException, int expectedErrorCode) {
            super(4);
            this.entryId = entryId;
            this.expectException = expectException;
            this.context = context;
            this.numOfEntry = numOfEntry;
            this.expectErrorCode = expectedErrorCode;
            this.data = data;

            this.useCallBack = useCallback;
        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // {data, useCallback, numOfEntry, context, expectException, expectedErrorCode}
                    ///valid
                    {new byte[]{1, 2, 3, 4}, true, 1, new Object(), false, BKException.Code.OK}, //callBack
                    {new byte[]{1, 2, 3, 4}, false, 1, new Object(), false, BKException.Code.OK}, //callBack
                    {new byte[0], true, 1, new Object(), false, BKException.Code.OK}, //no callBack
                    /// invalid
                    {new byte[]{1, 2, 3, 4}, true, 0, new Object(), true, BKException.Code.NoSuchEntryException}, //numOfEntry = 0
                    {new byte[0], true, -1, new Object(), true, BKException.Code.NoSuchEntryException}, //numOfEntry = -1




            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());

            if(numOfEntry == 0){
                return;
            }else{
                for (int i = 0; i < numOfEntry; i++) {
                    ledgerHandle.addEntry(("Entry " + i).getBytes()); //entryId assume l'id della ultima entry inserita
                }
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



        // Test principale
        @Test
        public void testAsyncAddEntry() {
            try {
                if(useCallBack) {

                    AtomicInteger resultCode = new AtomicInteger();
                    AtomicBoolean isComplete = new AtomicBoolean(false);
                    AtomicLong rEntry = new AtomicLong();
                    long lastEntryId = ledgerHandle.getLastAddConfirmed();
                    ledgerHandle.asyncAddEntry(this.data, (rc, ledgerHandle, entryId, ctx) -> {

                        rEntry.set(entryId);
                        isComplete.set(true);
                    }, context);

                    Awaitility.await().untilTrue(isComplete);
                    if (resultCode.get() == BKException.Code.OK) {
                        if(!expectException) {
                            assertNotNull("Entries should not be null on successful read", rEntry.get());
                            assertEquals("Expected successful read", this.numOfEntry, rEntry.get());
                            // Verifica che i byte inseriti siano corretti
                            Enumeration<LedgerEntry> entries = ledgerHandle.readEntries(rEntry.get(), rEntry.get());
                            LedgerEntry entry = entries.nextElement();

                            assertArrayEquals("Data should match the bytes added",
                                    this.data, entry.getEntry());
                        }
                        else{
                            assertEquals("Entries should be 0 on failure", 0, rEntry.get());
                        }

                    }

                }else{
                    ledgerHandle.asyncAddEntry(this.data, null, context);
                    if (expectException) {
                        fail("Expected exception, but method executed successfully.");
                    }
                }

            }catch (ArrayIndexOutOfBoundsException e){
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof ArrayIndexOutOfBoundsException);
                }
            }
            catch (NullPointerException e) {
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
    } //controllato con jacoco

//    @Ignore
    @RunWith(Parameterized.class)
    public static class LedgerHandleAsyncReadLastConfirmedTest extends BookKeeperClusterTestCase {

        LedgerHandle ledgerHandle;
        private final boolean expectException;
        private final Object context;
        private final int numOfEntry;
        private final int expectErrorCode;
        private final boolean useCallBack;
        private final boolean useV2Protocol;
        private BookKeeper bookk;
        private final boolean close;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleAsyncReadLastConfirmedTest(boolean close, boolean useV2Protocol, boolean useCallback, int numOfEntry,
                                                              Object context, boolean expectException, int expectedErrorCode) {
            super(4);
            this.expectException = expectException;
            this.context = context;
            this.numOfEntry = numOfEntry;
            this.expectErrorCode = expectedErrorCode;
            this.useV2Protocol = useV2Protocol;
            this.useCallBack = useCallback;
            this.close = close;
        }


        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // {useCallback, numOfEntry, context, expectException, expectedErrorCode}
                    ///valid
                    {false, false, true, 5, new Object(), false, BKException.Code.OK}, //callBack
                    {false, false, false, 5, new Object(), false, BKException.Code.OK}, //no callBack
                    {false, false, true, 0, new Object(), true, BKException.Code.NoSuchEntryException},
                    ///invalid
                    {false, false, true, -1, new Object(), true, BKException.Code.NoSuchEntryException},

                    // aggiunto per jacoco
                    {false, true, true, 5, new Object(), false, BKException.Code.OK}, //callBack
                    {true, true, true, 5, new Object(), false, BKException.Code.OK},
            });
        }


        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            if(useV2Protocol){

                ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
                conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
                conf.setExplictLacInterval(1);
                conf.setUseV2WireProtocol(useV2Protocol);
                bookk = new BookKeeper(conf);
                ledgerHandle = bookk.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());
            }
            else{
                ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());
            }


            if(numOfEntry == 0){
                return;
            }else{
                for (int i = 0; i < numOfEntry; i++) {
                    ledgerHandle.addEntry(("Entry " + i).getBytes()); //entryId assume l'id della ultima entry inserita
                }
            }
            if(close){
                ledgerHandle.close();
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



        // Test principale
        @Test
        public void testAsyncReadLastConfirmed() {
            try {
                if(useCallBack) {

                    AtomicInteger resultCode = new AtomicInteger();
                    AtomicBoolean isComplete = new AtomicBoolean(false);
                    AtomicLong rLAC = new AtomicLong();
                    ledgerHandle.getLastAddConfirmed();
                    ledgerHandle.asyncReadLastConfirmed( (rc, lastConfirmed, ctx) -> {
                        resultCode.set(rc);
                        rLAC.set(lastConfirmed);

                        isComplete.set(true);

                        if (resultCode.get() == BKException.Code.OK) {
                            if(!expectException)
                                assertEquals("Expected successful read", ledgerHandle.getLastAddConfirmed(), rLAC.get());
                            else{
                                assertEquals("Entries should be null on failure", -1L, rLAC.get());
                            }

                        } else {
                            assertEquals("Entries should be null on failure", -1L, rLAC.get());

                        }


                    }, context);

                    //Awaitility.await().untilTrue(isComplete);


                }else{
                    ledgerHandle.asyncReadLastConfirmed(null, context);
                    if (expectException) {
                        fail("Expected exception, but method executed successfully.");
                    }
                }

            }catch (ArrayIndexOutOfBoundsException e){
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof ArrayIndexOutOfBoundsException);
                }
            }
            catch (NullPointerException e) {
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
    } //aumentata coverage con jacoco

//    @Ignore
    @RunWith(Parameterized.class)
    public static class LedgerHandleReadLastEntryTest extends BookKeeperClusterTestCase {

        LedgerHandle ledgerHandle;
        private final boolean expectException;
        private final int numOfEntry;
        private final int expectErrorCode;

        // Costruttore per inizializzare i parametri del test
        public LedgerHandleReadLastEntryTest(int numOfEntry, boolean expectException, int expectedErrorCode) {
            super(4);
            this.expectException = expectException;
            this.numOfEntry = numOfEntry;
            this.expectErrorCode = expectedErrorCode;

        }
        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    ///valid
                    {5, false, BKException.Code.OK},
                    {1, false, BKException.Code.OK},
                    ///invalid
                    {0, true, BKException.Code.NoSuchEntryException},
                    {-1, true, BKException.Code.NoSuchEntryException},
            });
        }




        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pwd".getBytes());

            if(numOfEntry == 0){
                return;
            }else{
                for (int i = 0; i < numOfEntry; i++) {
                    ledgerHandle.addEntry(("Entry " + i).getBytes());
                }
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



        // Test principale
        @Test
        public void testReadLastEntry() {
            try {


                LedgerEntry le = ledgerHandle.readLastEntry();
                if (!expectException) {
                    assertNotNull("Entries should not be null on successful read", le);
                } else {
                    assertNull("Entries should be null on failure", le);
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
    } // 100% coverage

}
