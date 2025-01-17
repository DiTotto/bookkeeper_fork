package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.helper.TmpDirs;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

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
                    {createConfig("256.256.256.256", "eth0"), true},  // advertisedAddress non risolvibile
                    {createConfig("127.0.0.1", "eth99"), true}        // listeningInterface non valida
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
                    {createConfig("256.256.256.256", "bookie-123"), true} // advertisedAddress non risolvibile

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

}
