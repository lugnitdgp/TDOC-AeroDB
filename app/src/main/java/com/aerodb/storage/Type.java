package com.aerodb.storage;

import java.nio.charset.StandardCharsets;

public enum Type {
    INT,
    STRING;

    public int getLen() {
        if (this == INT) {
            return 4;
        }
        return 0;

    }

    // Helper to parse bytes into a Java Objectdepending upon the type
    public Object parse(byte[] data, int offset) {
        if (this == INT) {
            return (data[offset] << 24) | ((data[offset + 1] & 0xFF) << 16) |
                    ((data[offset + 2] & 0xFF) << 8) | (data[offset + 3] & 0xFF);
        } else {
            return new String(data, StandardCharsets.UTF_8);
        }
    }
}
