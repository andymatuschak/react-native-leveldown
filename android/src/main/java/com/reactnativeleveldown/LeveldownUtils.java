package com.reactnativeleveldown;

import android.os.Build;

import java.nio.charset.StandardCharsets;

class LeveldownUtils {
    static byte[] stringToByteArray(String string) {
        if (string == null) {
            return null;
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT) {
            return string.getBytes(StandardCharsets.UTF_8);
        } else {
            // default on Android is UTF-8, see https://stackoverflow.com/questions/7947871/convert-a-string-to-a-byte-array-and-then-back-to-the-original-string
            return string.getBytes();
        }
    }

    static String byteArrayToString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT) {
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            // default on Android is UTF-8, see https://stackoverflow.com/questions/7947871/convert-a-string-to-a-byte-array-and-then-back-to-the-original-string
            return new String(bytes);
        }
    }
}

