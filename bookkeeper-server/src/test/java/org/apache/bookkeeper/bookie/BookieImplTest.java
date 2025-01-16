package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(value = Enclosed.class)
public class BookieImplTest {

    @RunWith(Parameterized.class)
    public static class BookieImplGetBookieAddressTest {

        private final ServerConfiguration conf;
        private final boolean expectException;

        // Costruttore per inizializzare i parametri del test
        public BookieImplGetBookieAddressTest(ServerConfiguration conf, boolean expectException) {
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
                    {createConfig(null, null), true},            // no advertisedAddress, no listeningInterface
                    {createConfig(null, "invalidIface"), true},  // no advertisedAddress, listeningInterface non risolvibile

                    // Boundary
                    {createConfig("256.256.256.256", "eth0"), true},  // advertisedAddress non risolvibile
                    {createConfig("127.0.0.1", "eth99"), true}        // listeningInterface non valida
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
}
