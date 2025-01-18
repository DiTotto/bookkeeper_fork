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

        ByteBuf entry = Unpooled.buffer(dataSize);
        entry.writeBytes(data);

        return entry;
    }





}