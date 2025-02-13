package org.apache.bookkeeper.helper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class EntryBuilder {

    // method to create a valid entry
    public static ByteBuf createValidEntry() {
        long ledgerId = 0L;
        long entryId = 0L;
        long lastConfirmed = 0L;
        byte[] data = "ValidData".getBytes();
        byte[] authCode = "ValidAuth".getBytes();

        int metadataSize = Long.BYTES * 3; // ledgerId, entryId, lastConfirmed
        int dataSize = data.length;
        int authCodeSize = authCode.length;

        ByteBuf entry = Unpooled.buffer(metadataSize + dataSize + authCodeSize);

        // 1. metadata
        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeLong(lastConfirmed);

        // 2. data
        entry.writeBytes(data);

        // 3. auth code
        entry.writeBytes(authCode);

        return entry;
    }
    // method to create a valid entry
    public static ByteBuf createValidEntryWithLedgerId(long i) {
        long ledgerId = i;
        long entryId = 0L;
        long lastConfirmed = 0L;
        byte[] data = "ValidData".getBytes();
        byte[] authCode = "ValidAuth".getBytes();

        int metadataSize = Long.BYTES * 3; // ledgerId, entryId, lastConfirmed
        int dataSize = data.length;
        int authCodeSize = authCode.length;

        ByteBuf entry = Unpooled.buffer(metadataSize + dataSize + authCodeSize);

        // 1. metadata
        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeLong(lastConfirmed);

        // 2. data
        entry.writeBytes(data);

        // 3. auth code
        entry.writeBytes(authCode);

        return entry;
    }

    // invalid entry builder --> invalid metadata
    public static ByteBuf createInvalidEntry() {
        long ledgerId = -1L;
        long entryId = -1L;
        long lastConfirmed = -1L;
        byte[] data = "InvalidData".getBytes();
        byte[] authCode = "InvalidAuth".getBytes();


        int metadataSize = Long.BYTES * 3; // ledgerId, entryId, lastConfirmed
        int dataSize = data.length;
        int authCodeSize = authCode.length;

        ByteBuf entry = Unpooled.buffer(metadataSize + dataSize + authCodeSize);

        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeLong(lastConfirmed);

        entry.writeBytes(data);
        entry.writeBytes(authCode);

        return entry;
    }

    // invalid entry builder --> metadata absent
    public static ByteBuf createInvalidEntryWithoutMetadata() {
        byte[] data = "DataWithoutMetadata".getBytes();

        int dataSize = data.length;


        //entry without ledgerId, entryId, lastConfirmed
        ByteBuf entry = Unpooled.buffer(dataSize);
        entry.writeBytes(data);

        return entry;
    }


    public static long getLedgerId(ByteBuf entry) {
        int pointer = entry.readerIndex();
        try{
            entry.readerIndex(0);
            return entry.readLong();
        }
        finally {
            entry.readerIndex(pointer);
        }
    }


    public static long getEntryId(ByteBuf entry) {
        int pointer = entry.readerIndex();
        try{
            entry.readerIndex(Long.BYTES);
            return entry.readLong();
        }
        finally {
            entry.readerIndex(pointer);
        }
    }

    public static boolean isValid(ByteBuf entry) {
        int readableBytes = entry.readableBytes();
        int metadataSize = Long.BYTES * 3;
        byte[] validAuth = "Valid".getBytes();
        byte[] validData = "Valid".getBytes();

        if (readableBytes >= metadataSize + validAuth.length + validData.length) {
            int readerIndex = entry.readerIndex();
            try {
                entry.readerIndex(metadataSize);


                byte[] data = new byte[validData.length];
                entry.readBytes(data);
                if (!new String(data).equals(new String(validData))) {
                    return false;
                }


                byte[] auth = new byte[validAuth.length];
                entry.readBytes(auth);
                return new String(auth).equals(new String(validAuth));
            } finally {
                entry.readerIndex(readerIndex);
            }
        }
        return false;
    }
}