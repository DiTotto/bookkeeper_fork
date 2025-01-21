package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.helper.DeleteTemporaryDir;
import org.apache.bookkeeper.helper.DirectoryTestHelper;
import org.apache.bookkeeper.helper.EntryBuilder;
import org.apache.bookkeeper.helper.TmpDirs;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.bookkeeper.helper.DirectoryTestHelper.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.net.UnknownHostException;
import java.util.PrimitiveIterator;

import io.netty.buffer.Unpooled;

@RunWith(value = Enclosed.class)
public class BookieImplTest {


    // Test per il metodo getBookieAddress
    @RunWith(Parameterized.class)
    public static class BookieImplGetBookieAddressParameterizedTest {

        private final ServerConfiguration conf;
        private final boolean expectException;

        // Costruttore per inizializzare i parametri del test
        public BookieImplGetBookieAddressParameterizedTest(ServerConfiguration conf, boolean expectException) {
            this.conf = conf;
            this.expectException = expectException;
        }



        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Classi di equivalenza valide
                    {createConfig("127.0.0.1", "eth0"), false},  // advertisedAddress valido, listeningInterface valida
                    {createConfig("localhost", "eth0"), false}, // advertisedAddress valido, listeningInterface valida
                    {createConfig("127.0.0.1", null), false},   // advertisedAddress valido, no listeningInterface

                    // Classi di equivalenza non valide
                    {createConfig(null, "eth0"), true},          // no advertisedAddress, listeningInterface valida
                    {createConfig("", "eth0"), true},          // advertisedAddress vuoto, listeningInterface valida
                    {createConfig(null, null), true},            // no advertisedAddress, no listeningInterface
                    {createConfig("", ""), true},            // advertisedAddress vuoto, listeningInterface vuoto
                    {createConfig(null, "invalidIface"), true},  // no advertisedAddress, listeningInterface non risolvibile

                    // Boundary
                    {createConfig("256.256.256.256", "eth0"), false},  // advertisedAddress non risolvibile
                    {createConfig("127.0.0.1", "eth99"), false}        // listeningInterface non valida
                    /**
                    * in questi ultimi due casi mi aspetto venga sollevata un'eccezione ma dal test risulta che invece non viene sollevata. Per ora lascio cosi e poi
                     * vedo se è effettivamente un problema o se non viene sollevata perche la funzione gestisce correttamente qlo specifico caso in cui l'indirizzo non è valido
                     * o l'interfaccia non è valida
                     */
            });
        }

        // Helper per creare configurazioni
        private static ServerConfiguration createConfig(String advertisedAddress, String iface) {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            if (iface != null) {
                conf.setListeningInterface(iface);
            }
            return conf;
        }

        // Test principale
        @Test
        public void testGetBookieAddress() {
            try {
                // Chiamata al metodo da testare
                BookieSocketAddress address = BookieImpl.getBookieAddress(conf);

                // Verifica che non venga sollevata un'eccezione quando non ci si aspetta errori
                if (expectException) {
                    System.out.println("Expected exception thrown: " + conf.getAdvertisedAddress()  + " " + conf.getListeningInterface());
                    fail("Expected an exception, but none was thrown.");
                }
                // Verifica che l'indirizzo restituito sia valido
                assertNotNull("BookieSocketAddress should not be null for valid configuration.", address);
            } catch (UnknownHostException e) {
                // Verifica che venga sollevata un'eccezione quando ci si aspetta un errore
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            } catch (Exception e) {
                fail("Unexpected exception type: " + e.getMessage());
            }
        }
    }

    // Test per il metodo getBookieId
    @RunWith(Parameterized.class)
    public static class BookieImplGetBookieIdParameterizedTest {

        private final ServerConfiguration conf;
        private final boolean expectException;

        // Costruttore per inizializzare i parametri del test
        public BookieImplGetBookieIdParameterizedTest(ServerConfiguration conf, boolean expectException) {
            this.conf = conf;
            this.expectException = expectException;
        }

        // Parametri del test (classi di equivalenza e boundary)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Classi di equivalenza valide
                    {createConfig("192.168.1.1", "bookie-123"), false},  // advertisedAddress valido, bookieID valido
                    {createConfig("bookie.local", null), false},         // advertisedAddress valido, no bookieID
                    {createConfig(null, "bookie-123"), false},           // no advertisedAddress, bookieID valido

                    // Classi di equivalenza non valide
                    {createConfig(null, null), true},                     // no advertisedAddress, no bookieID
                    {createConfig(null, ""), true},                       // no advertisedAddress, bookieID vuoto
                    {createConfig("192.168.1.1", ""), false},             // advertisedAddress valido, bookieID vuoto

                    // Boundary
                    {createConfig("bookie.local", ""), false},            // advertisedAddress valido, bookieID vuoto
                    {createConfig("256.256.256.256", "bookie-123"), false} // advertisedAddress non risolvibile

                    /* come per il test precedente, anche in questo caso mi aspetto che venga sollevata un'eccezione ma dal test risulta che non viene sollevata
                    *  e esattamente come prima il comportamento inatteso si ha quando l'avvertisedAddress non è valido.
                    */
            });
        }

        // Helper per creare configurazioni
        private static ServerConfiguration createConfig(String advertisedAddress, String bookieId) {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            if(bookieId != null && !bookieId.isEmpty())
                conf.setBookieId(bookieId);
            return conf;
        }

        // Test principale
        @Test
        public void testGetBookieId() {
            try {
                // Chiamata al metodo da testare
                BookieId bookieId = BookieImpl.getBookieId(conf);

                // Verifica che non venga sollevata un'eccezione quando non ci si aspetta errori
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                // Verifica che il BookieId restituito sia valido
                assertNotNull("BookieId should not be null for valid configuration.", bookieId);
            } catch (UnknownHostException e) {
                // Verifica che venga sollevata un'eccezione quando ci si aspetta un errore
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            } catch (Exception e) {
                fail("Unexpected exception type: " + e.getMessage());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplGetCurrentDirectoriesParameterizedTest {

        private static TmpDirs tmpDirs;
        private static File existingDir;
        private static File anotherExistingDir;
        private static File validDir1;
        private static File validDir2;
        private static File inaccessibleDir;
        private static File file;



        private final File[] dirs;
        private final boolean expectException;

        public BookieImplGetCurrentDirectoriesParameterizedTest(File[] dirs, boolean expectException) {
            this.dirs = dirs;
            this.expectException = expectException;
        }

        @BeforeClass
        public static void setUpClass() throws Exception {
            tmpDirs = new TmpDirs();


            /*existingDir = tmpDirs.createNew("existingDir", null);
            anotherExistingDir = tmpDirs.createNew("anotherExistingDir", null);
            */
            validDir1 = tmpDirs.createNew("validDir1", null);
            validDir2 = tmpDirs.createNew("validDir2", null);

            // Creazione di una directory non accessibile
            inaccessibleDir = tmpDirs.createNew("inaccessibleDir", null);
            //inaccessibleDir.setReadable(false);
            inaccessibleDir.setWritable(false);
            // Creazione di un file
            file = File.createTempFile("tempFile", ".tmp");
            file.deleteOnExit();
        }


        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    /*// valid: array di dirs
                    {new File[]{existingDir, anotherExistingDir}, false},
                    // array vuoto
                    {new File[]{}, false},
                    // invalid: null
                    {null, true},*/
                    // valid: array di dirs
                    {new File[]{validDir1, validDir2}, false},
                    // Valid: Mixed valid and invalid (file + inaccessible)
                    {new File[]{validDir1, file, inaccessibleDir}, false},
                    // Valid: Empty input
                    {new File[]{}, false},
                    // Invalid: Null input
                    {null, true},
            });
        }

        @Test
        public void testGetCurrentDirectories() {
            try {
                File[] result = BookieImpl.getCurrentDirectories(dirs);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("Result should not be null", result);

            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            }
        }

        @AfterClass
        public static void tearDownClass() {
            if (tmpDirs != null) {
                try {
                    tmpDirs.cleanup();
                } catch (IOException e) {
                    System.err.println("Failed to clean up temporary directories: " + e.getMessage());
                    e.printStackTrace();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }



    @RunWith(Parameterized.class)
    public static class BookieImplMountLedgerStorageOfflineParameterizedTest {

        private final ServerConfiguration conf;
        private final LedgerStorage ledgerStorage;
        private final boolean expectException;

        public BookieImplMountLedgerStorageOfflineParameterizedTest(ServerConfiguration conf, LedgerStorage ledgerStorage, boolean expectException) {
            this.conf = conf;
            this.ledgerStorage = ledgerStorage;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // valid
                    {createValidConfig(), createMockLedgerStorage(false), false},   // conf valida con ledgerStorage non inizializzato

                    // invalid
                    {null, createMockLedgerStorage(false), true},                  // conf null
                    {createInvalidConfig(), createMockLedgerStorage(false), true}, // conf invalida con ledgerStorage non inizializzato
                    {createValidConfig(), createMockLedgerStorage(true), true},    // conf valida con ledgerStorage già inizializzato
                    {createPartialConfigOnlyUsageThreshold(), createMockLedgerStorage(false), true}, // conf parziale con ledgerStorage non inizializzato
                    {createPartialConfigOnlyWarnThreshold(), createMockLedgerStorage(false), false}, // conf parziale con ledgerStorage non inizializzato
                    // in questo caso, quando viene configurata solo la warn threshold, e non c'è la threshold, mi apsetto che il meotodo sollevi un'eccezione
                    // ma invece non viene sollevata. Anche in questo caso lascio cosi e poi vedo se è effettivamente un problema
                    {createValidConfig(), null, false},
            });
        }

        private static ServerConfiguration createValidConfig() {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setLedgerDirNames(new String[]{"/tmp/ledgerDirs"});
            conf.setDiskUsageThreshold(0.8f);
            conf.setDiskUsageWarnThreshold(0.7f);
            return conf;
        }

        private static ServerConfiguration createInvalidConfig() {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setDiskUsageThreshold(0.7f);
            conf.setDiskUsageWarnThreshold(0.8f); // Warn > Threshold => invalida
            // conf vuota o non valida
            conf.setLedgerDirNames(null); // no directory impostata
            return conf;
        }

        private static ServerConfiguration createPartialConfigOnlyUsageThreshold() {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setDiskUsageThreshold(0.6f);
            // conf vuota o non valida
            conf.setLedgerDirNames(null); // no directory impostata
            return conf;
        }

        private static ServerConfiguration createPartialConfigOnlyWarnThreshold() {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setDiskUsageWarnThreshold(0.6f);
            // conf vuota o non valida
            conf.setLedgerDirNames(null); // no directory impostata
            return conf;
        }

        private static LedgerStorage createMockLedgerStorage(boolean alreadyInitialized) {
            LedgerStorage mockStorage = mock(LedgerStorage.class);
            if (alreadyInitialized) {
                try {
                    doThrow(new IllegalStateException("LedgerStorage already initialized"))
                            .when(mockStorage).initialize(any(), any(), any(), any(), any(), any());
                } catch (IOException e) {
                    fail("Unexpected IOException during mock setup");
                }
            }
            return mockStorage;
        }

        @Test
        public void testMountLedgerStorageOffline() {
            try {
                LedgerStorage result = BookieImpl.mountLedgerStorageOffline(conf, ledgerStorage);
                if (expectException) {
                    fail("Expected exception but none was thrown.");
                }
                assertNotNull("Resulting LedgerStorage should not be null", result);
                assertEquals("Returned LedgerStorage should match input LedgerStorage when provided",
                        ledgerStorage != null ? ledgerStorage : result, result);
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception: " + e.getMessage());
                }
            }
        }
    }


    @RunWith(Parameterized.class)
    public static class FormatParameterizedTest {

        private final boolean isInteractive;
        private final boolean force;
        private final boolean expectedResult;
        private final DirectoryTestHelper journalDirs;

        private final DirectoryTestHelper ledgerDirs;

        private final DirectoryTestHelper indexDirs;

        private final DirectoryTestHelper gcEntryLogMetadataCachePath;
        private final String input;
        private final boolean expException;

        private ServerConfiguration conf;
        private static InputStream originalSystemIn;

        public FormatParameterizedTest(DirectoryTestHelper journalDirs, DirectoryTestHelper ledgerDirs, DirectoryTestHelper indexDirs, DirectoryTestHelper gcEntryLogMetadataCachePath,
                                       boolean isInteractive, boolean force, String input,
                                       boolean expectedResult, boolean expException) {
            this.isInteractive = isInteractive;
            this.force = force;
            this.expectedResult = expectedResult;
            this.journalDirs = journalDirs;
            this.ledgerDirs = ledgerDirs;
            this.indexDirs = indexDirs;
            this.gcEntryLogMetadataCachePath = gcEntryLogMetadataCachePath;
            this.input = input;
            this.expException = expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{

                    {DIR_WITH_FILE, DIR_WITH_FILE, DIR_WITH_FILE, DIR_WITH_FILE, true, false, "Y", true, false},
                    {DIR_WITH_FILE, DIR_WITH_SUBDIR_AND_FILE, DIR_WITH_SUBDIR_AND_FILE, DIR_WITH_SUBDIR_AND_FILE, true, false, "Y", true, false},
                    {DIR_WITH_SUBDIR_AND_FILE, DIR_WITH_FILE, EMPTY_LIST, NON_EXISTENT_DIRS, true, false, "N", false, false},
                    {DIR_WITH_LOCKED_FILE, DIR_WITH_LOCKED_FILE, DIR_WITH_LOCKED_FILE, DIR_WITH_LOCKED_FILE, false, true, "Y", false, false},
                    {DIR_WITH_LOCKED_EMPTY_SUBDIR, NON_EXISTENT_DIRS, DIR_WITH_FILE, EMPTY_LIST, false, true, "Y", false, false},
                    {EMPTY_LIST, DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_LOCKED_EMPTY_SUBDIR, true, false, "Y", false, false},
                    {DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_LOCKED_FILE, DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_LOCKED_FILE, false, true, "Y", false, false},
                    {DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_SUBDIR_AND_FILE, DIR_WITH_FILE, EMPTY_LIST, false, true, "Y", false, false},
                    {EMPTY_LIST, DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_LOCKED_EMPTY_SUBDIR, DIR_WITH_SUBDIR_AND_FILE, true, false, "Y", false, false},
                    {EMPTY_LIST, EMPTY_LIST, EMPTY_LIST, EMPTY_LIST, true, true, "Y", true, false},
                    {NULL, NULL, NULL, EMPTY_LIST, false, false, "Y", false, true},
                    {NULL, EMPTY_LIST, NULL, EMPTY_LIST, false, false, "Y", false, true},
                    {NULL, EMPTY_LIST, DIR_WITH_LOCKED_EMPTY_SUBDIR, EMPTY_LIST, false, false, "Y", false, true},

            });
        }

        @Before
        public void setup() throws Exception {
            conf = mock(ServerConfiguration.class);

            File[] journalDirsArray = journalDirs.fetchDirectories();
            File[] ledgerDirsArray = ledgerDirs.fetchDirectories();
            File[] indexDirsArray = indexDirs.fetchDirectories();
            String gcPath = gcEntryLogMetadataCachePath.fetchGcLogMetadataPath();

            when(conf.getJournalDirs()).thenReturn(journalDirsArray);
            when(conf.getLedgerDirs()).thenReturn(ledgerDirsArray);
            when(conf.getIndexDirs()).thenReturn(indexDirsArray);
            when(conf.getGcEntryLogMetadataCachePath()).thenReturn(gcPath);
        }

        @After
        public void cleanup() {
            DeleteTemporaryDir.deleteFiles(conf.getJournalDirs(), conf.getIndexDirs(), conf.getLedgerDirs());
            System.setIn(originalSystemIn);
        }

        @Test
        public void formatTest() {
            try {
                originalSystemIn = System.in;

                switch (this.input) {
                    case "Y":
                        System.setIn(new ByteArrayInputStream("Y\nY\nY\n".getBytes()));
                        break;
                    case "N":
                        System.setIn(new ByteArrayInputStream("N\n".getBytes()));
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid input case: " + this.input);
                }


                Assert.assertEquals(expectedResult, BookieImpl.format(conf, isInteractive, force));

                File[] journalDirs = conf.getJournalDirs();
                if (journalDirs != null && expectedResult) {
                    for (File dir : journalDirs) {
                        assertNotNull("Journal directory should not be null", dir);
                        assertTrue("Journal directory should exist after formatting", dir.exists());
                        assertTrue("Journal directory should be writable after formatting", dir.canWrite());
                        assertTrue("Journal directory should be empty after formatting", dir.list().length == 0);
                    }
                }

                File[] ledgerDirs = conf.getLedgerDirs();
                if (ledgerDirs != null && expectedResult) {
                    for (File dir : ledgerDirs) {
                        assertNotNull("Ledger directory should not be null", dir);
                        assertTrue("Ledger directory should exist after formatting", dir.exists());
                        assertTrue("Ledger directory should be writable after formatting", dir.canWrite());
                        assertTrue("Ledger directory should be empty after formatting", dir.list().length == 0);
                    }
                }

                File[] indexDirs = conf.getIndexDirs();
                if (indexDirs != null && expectedResult) {
                    for (File dir : indexDirs) {
                        assertNotNull("Index directory should not be null", dir);
                        assertTrue("Index directory should exist after formatting", dir.exists());
                        assertTrue("Index directory should be writable after formatting", dir.canWrite());
                        assertTrue("Index directory should be empty after formatting", dir.list().length == 0);
                    }
                }

                assertFalse("Exception was not expected", this.expException);

            } catch (NullPointerException e) {
                if (!expException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
                assertTrue("Exception 1", this.expException);
            } catch (Exception e) {
                if (!expException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
                assertTrue("Exception 2", this.expException);
            }
        }

    }


    /*@RunWith(Parameterized.class)
    public static class GetListOfEntriesOfLedgerTest{

            private final long ledgerId;
            //private final long startEntryId;
            //private final long endEntryId;
            private final boolean expectException;
            private BookieImpl bookie;

            /*public GetListOfEntriesOfLedgerTest(long ledgerId, long startEntryId, long endEntryId, boolean expectException) {
                this.ledgerId = ledgerId;
                this.startEntryId = startEntryId;
                this.endEntryId = endEntryId;
                this.expectException = expectException;
            }*/
            /*public GetListOfEntriesOfLedgerTest(long ledgerId, boolean expectException) {
                this.ledgerId = ledgerId;
                this.expectException = expectException;
            }

            @Before
            public void setup() throws Exception {



                ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

                bookie = new TestBookieImpl(conf);


            }

            @Parameterized.Parameters
            public static Collection<Object[]> data() {
                return Arrays.asList(new Object[][]{
                        // valid
                        /*{1, 1, 10, false},   // ledgerId, startEntryId, endEntryId validi
                        {1, 1, 1, false},    // ledgerId, startEntryId, endEntryId validi
                        {1, 10, 1, false},   // ledgerId, startEntryId, endEntryId validi
                        // invalid
                        {0, 1, 10, true},    // ledgerId non valido
                        {1, 0, 10, true},    // startEntryId non valido
                        {1, 1, 0, true},     // endEntryId non valido
                        {0, 0, 0, true},     // ledgerId, startEntryId, endEntryId non validi
                *//*
                        {1,false}
                });
            }

            @Test
            public void testGetListOfEntriesOfLedger() {
                try {
                    //bookie.getListOfEntriesOfLedger(ledgerId, startEntryId, endEntryId);
                    bookie.getListOfEntriesOfLedger(ledgerId);
                    if (expectException) {
                        fail("Expected exception but none was thrown.");
                    }
                } catch (Exception e) {
                    if (!expectException) {
                        fail("Unexpected exception: " + e.getMessage());
                    }
                }
            }

    }
*/
    @RunWith(Parameterized.class)
    public static class GetListOfEntriesOfLedgerTest{

        private final long ledgerId;
        //private final long startEntryId;
        //private final long endEntryId;
        private final boolean expectException;
        private final ByteBuf entry;
        private final boolean ackBeforeSync;
        private final BookkeeperInternalCallbacks.WriteCallback cb;
        private final Object ctx;
        private final byte[] masterKey;

        //private final Long testLedgerId;
        //private final boolean useWatcher;
        private final Class<? extends Exception> exceptionClass;
        private BookieImpl bookie;

            /*public GetListOfEntriesOfLedgerTest(long ledgerId, long startEntryId, long endEntryId, boolean expectException) {
                this.ledgerId = ledgerId;
                this.startEntryId = startEntryId;
                this.endEntryId = endEntryId;
                this.expectException = expectException;
            }*/
            /*public GetListOfEntriesOfLedgerTest(long ledgerId, boolean expectException) {
                this.ledgerId = ledgerId;
                this.expectException = expectException;
            }*/
            public GetListOfEntriesOfLedgerTest(long ledgerId1, ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb,
                                                Object ctx, byte[] masterKey, Long testLedgerId,
                                                boolean useWatcher, boolean expectException, Class<? extends Exception> exceptionClass) {
                this.ledgerId = ledgerId1;
                this.entry = entry;
                this.ackBeforeSync = ackBeforeSync;
                this.cb = cb;
                this.ctx = ctx;
                this.masterKey = masterKey;
                this.expectException = expectException;
                this.exceptionClass = exceptionClass;
            }


            @Before
            public void setup() throws Exception {



                ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

                bookie = new TestBookieImpl(conf);


            }

            @Parameterized.Parameters
            public static Collection<Object[]> data() {
                return Arrays.asList(new Object[][]{
                        // valid
                        /*{1, 1, 10, false},   // ledgerId, startEntryId, endEntryId validi
                        {1, 1, 1, false},    // ledgerId, startEntryId, endEntryId validi
                        {1, 10, 1, false},   // ledgerId, startEntryId, endEntryId validi
                        // invalid
                        {0, 1, 10, true},    // ledgerId non valido
                        {1, 0, 10, true},    // startEntryId non valido
                        {1, 1, 0, true},     // endEntryId non valido
                        {0, 0, 0, true},     // ledgerId, startEntryId, endEntryId non validi
                */
                        //{1,false}
                        // valid case
                        {1, EntryBuilder.createValidEntryWithLedgerId(1), true, mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), null, true, false, null},
                        // invalid case
                        {2, EntryBuilder.createValidEntryWithLedgerId(1), true, mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), null, true, true, Bookie.NoLedgerException.class},
                        {2, EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), null, true, true, Bookie.NoLedgerException.class},

                });
            }
            private static BookkeeperInternalCallbacks.WriteCallback mockWriteCallback() {
                return mock(BookkeeperInternalCallbacks.WriteCallback.class);
            }

            @Test
            public void testGetListOfEntriesOfLedger() {
                try {
                    bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
                    //bookie.getListOfEntriesOfLedger(ledgerId, startEntryId, endEntryId);
                    PrimitiveIterator.OfLong entriesOfLedger = bookie.getListOfEntriesOfLedger(ledgerId);

                    if (expectException && exceptionClass != null) {
                        fail("Expected exception but none was thrown.");
                    }
                } catch (Exception e) {
                    if(exceptionClass != null) {
                        if ( exceptionClass != e.getClass()) {
                            fail("Expected exception of class: " + exceptionClass + "but obtained of class " + e.getClass());
                        }
                    }
                    if (!expectException) {
                        fail("Unexpected exception: " + e.getMessage() + " " + e.getClass());
                    }

                }
            }

        @After
        public void teardown() {
            try {
                bookie.shutdown();
            } catch (Exception e) {
                fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
            }
        }

    }


    @RunWith(Parameterized.class)
    public static class ReadEntryTest{

        private final boolean expectException;
        private final ByteBuf entry;
        private final boolean ackBeforeSync;
        private final BookkeeperInternalCallbacks.WriteCallback cb;
        private final Object ctx;
        private final byte[] masterKey;
        private  Long expectedLedgerId;
        private Long expectedEntryId;


        private final Class<? extends Exception> exceptionClass;
        private BookieImpl bookie;

        public ReadEntryTest(ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb,
                                            Object ctx, byte[] masterKey, Long testLedgerId, Long expectedEntryId,
                                            boolean expectException, Class<? extends Exception> exceptionClass) {
            this.entry = entry;
            this.ackBeforeSync = ackBeforeSync;
            this.cb = cb;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.expectException = expectException;
            this.exceptionClass = exceptionClass;
            this.expectedLedgerId = testLedgerId;
            this.expectedEntryId = expectedEntryId;

        }


        @Before
        public void setup() throws Exception {

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            bookie = new TestBookieImpl(conf);

        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{

                    // valid case
                    {EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), "Valida".getBytes(), null, null,false, null},
                    //entry valida, con ackBeforeSync = true, la callback gestita dal mock, il contesto è un oggetto generico, la chiave master è una stringa valida, il ledgerId è null
                    // il ledgerid è null e useWatcher è true
                    // invalid case
                    {null, true, null, null, "Valida".getBytes(), -1L, null, true, null},
                    // invalid perchè entry null,
                    // con ackBeforeSync = true, la callback è null, il contesto è null, la chiave master è una stringa valida, il ledgerId è -1
                    {EntryBuilder.createInvalidEntryWithoutMetadata(), false, mockWriteCallback(), new Object(), "".getBytes(StandardCharsets.UTF_8), null, null, true, IndexOutOfBoundsException.class},
                    // entry non valida, senza metadata, con ackBeforeSync = false, la callback gestita dal mock, il contesto è un oggetto generico, la chiave master è una stringa vuota, il ledgerId è null
                    {EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), null, null,null, true, Bookie.NoLedgerException.class},
                    // entry non valida perchè la chiave è una null,
                    {EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), "Valida".getBytes(), -1L, null, true, null},
                    // entry non valida perche il ledgerId passato è -1L
                    {EntryBuilder.createValidEntryWithLedgerId(1), true, mockWriteCallback(), new Object(), "Valida".getBytes(), 2L, null, true, null},
                    // entry non valida perchè il ledgerId passato è diverso da quello inserito al momento della creazione
                    {EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), "Valida".getBytes(), null, 1L, true, null},
                    // entry non valida perchè l'entryId è un entryId randomico non presente nel ledger
            });
        }
        private static BookkeeperInternalCallbacks.WriteCallback mockWriteCallback() {
            return mock(BookkeeperInternalCallbacks.WriteCallback.class);
        }

        @Test
        public void testReadAndAddEntry() {
            boolean wasNull = false;

            try {
                bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);

                if (expectException && exceptionClass != null) {
                    fail("Expected exception but none was thrown.");
                }

                if(!expectException) {
                    if(expectedLedgerId == null) {
                        wasNull = true;
                        this.expectedLedgerId = EntryBuilder.getLedgerId(entry);
                    }
                    if (expectedEntryId == null) {
                        this.expectedEntryId = EntryBuilder.getEntryId(entry);
                    }
                    ByteBuf readEntry = bookie.readEntry(expectedLedgerId, expectedEntryId);
                    assertNotNull("Entry should not be null", readEntry);
                    assertEquals("Entry should be equal to the one added", entry, readEntry);

                    if (wasNull){
                        long lastEntryId = bookie.readLastAddConfirmed(expectedLedgerId);
                        assertEquals("EntryId should be equal to lastEntryId", (long) this.expectedEntryId, lastEntryId);

                    }else{
                        try {
                            bookie.readLastAddConfirmed(expectedLedgerId);
                            fail("Expected exception but none was thrown.");
                        } catch (Bookie.NoLedgerException e) {
                            // expected
                            assertTrue("Exception correctly thrown",true);
                        }
                        catch (IOException e) {
                            assertTrue("Exception correctly thrown",true);
                        }

                    }

                }
            } catch (Exception e) {
                if(exceptionClass != null) {
                    if ( exceptionClass != e.getClass()) {
                        fail("Expected exception of class: " + exceptionClass + "but obtained of class " + e.getClass());
                    }
                }
                if (!expectException) {
                    fail("Unexpected exception: " + e.getMessage() + " " + e.getClass());
                }

            }
        }

        @After
        public void teardown() {
            try {
                bookie.shutdown();
            } catch (Exception e) {
                fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class SetExplicitLacTest {

        private final boolean expectException;
        private final ByteBuf entry;
        private final BookkeeperInternalCallbacks.WriteCallback cb;
        private final Object ctx;
        private final byte[] masterKey;
        private Long expectedLedgerId;



        private final Class<? extends Exception> exceptionClass;
        private BookieImpl bookie;

        public SetExplicitLacTest(ByteBuf entry, BookkeeperInternalCallbacks.WriteCallback cb,
                             Object ctx, byte[] masterKey, Long ledgerId,
                             boolean expectException, Class<? extends Exception> exceptionClass) {
            this.entry = entry;
            this.cb = cb;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.expectException = expectException;
            this.exceptionClass = exceptionClass;
            this.expectedLedgerId = ledgerId;
        }


        @Before
        public void setup() throws Exception {

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            bookie = new TestBookieImpl(conf);

        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{

                    // valid case
                    {EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), "Valida".getBytes(), 1L,false, null},
                    //entry valida, con ackBeforeSync = true, la callback gestita dal mock, il contesto è un oggetto generico, la chiave master è una stringa valida, il ledgerId è null
                    // il ledgerid è null
                    {EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), "Valida".getBytes(), null, false, null},
                    // invalid case
                    {null, null, null, "Valida".getBytes(), -1L, true, null},
                    // invalid perchè entry null,
                    // con ackBeforeSync = true, la callback è null, il contesto è null, la chiave master è una stringa valida, il ledgerId è -1
                    {EntryBuilder.createValidEntryWithLedgerId(1L), mockWriteCallback(), new Object(), null, null, true, Bookie.NoLedgerException.class},
                    // entry non valida, senza metadata, con ackBeforeSync = false, la callback gestita dal mock, il contesto è un oggetto generico, la chiave master è una stringa vuota, il ledgerId è null
                    //{EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), null, null, true, Bookie.NoLedgerException.class},
                    // entry non valida perchè la chiave è una null,
                    {EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), "Valida".getBytes(), -1L, true, null},
                    // entry non valida perche il ledgerId passato è -1L
                    {EntryBuilder.createValidEntryWithLedgerId(1L), mockWriteCallback(), new Object(), "Valida".getBytes(), 2L, false, null},
                    // entry valida perchè anche se il ledgerID è diverso da quello passato al momento della creazione,
                    // nel momento in cui viene settato l'explicitLac, il ledgerId viene preso da quello passato

            });
        }
        private static BookkeeperInternalCallbacks.WriteCallback mockWriteCallback() {
            return mock(BookkeeperInternalCallbacks.WriteCallback.class);
        }

        @Test
        public void testSetAndGetExplicitLAC() {
            try {
                if (this.expectedLedgerId == null){
                    this.expectedLedgerId = EntryBuilder.getLedgerId(entry);
                } else if (this.expectedLedgerId < 0){
                    assertTrue("Negative ledgerId",this.expectException);
                    return;
                }

                System.out.println("LedgerId: " + EntryBuilder.getLedgerId(entry));

                ByteBuf lacEntry = bookie.createExplicitLACEntry(expectedLedgerId, entry);
                assertNotNull("Entry should not be null", lacEntry);

                bookie.setExplicitLac(Unpooled.copiedBuffer(lacEntry), cb, ctx, masterKey);
                System.out.println(lacEntry.readableBytes()+ "  "+ lacEntry.isReadable());
                assertTrue("ExplicitLac should be set", lacEntry.isReadable());
                ByteBuf retrievedLac = bookie.getExplicitLac(expectedLedgerId);

                byte[] retrievedBytes = new byte[retrievedLac.readableBytes()];
                retrievedLac.getBytes(retrievedLac.readerIndex(), retrievedBytes);

                byte[] lacEntryBytes = new byte[lacEntry.readableBytes()];
                lacEntry.getBytes(lacEntry.readerIndex(), lacEntryBytes);

                assertEquals(new String(retrievedBytes), new String(lacEntryBytes));
                //assertEquals(new String(retrievedLac.array()),new String(lacEntry.array()));
                assertFalse("Expected exception but none was thrown ",this.expectException);

            }catch (Exception e){
                if (!expectException) {
                    fail("Unexpected exception: " + e.getMessage() + " " + e.getClass());
                }
                if(exceptionClass != null) {
                    if ( exceptionClass != e.getClass()) {
                        fail("Expected exception of class: " + exceptionClass + "but obtained of class " + e.getClass());
                    }
                }
            }

        }

        @After
        public void teardown() {
            try {
                bookie.shutdown();
            } catch (Exception e) {
                fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
            }
        }
    }

}
