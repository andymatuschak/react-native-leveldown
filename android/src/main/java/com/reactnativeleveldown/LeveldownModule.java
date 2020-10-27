package com.reactnativeleveldown;

import android.util.Log;

import com.facebook.react.bridge.LifecycleEventListener;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.ReadableArray;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.WritableArray;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.WritableNativeArray;
import com.facebook.react.bridge.WritableNativeMap;
import com.github.hf.leveldb.LevelDB;
import com.github.hf.leveldb.WriteBatch;
import com.github.hf.leveldb.exception.LevelDBException;
import com.github.hf.leveldb.util.Bytes;
import com.github.hf.leveldb.util.SimpleWriteBatch;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class LeveldownModule extends ReactContextBaseJavaModule implements LifecycleEventListener {

    private final ReactApplicationContext reactContext;

    public LeveldownModule(ReactApplicationContext reactContext) {
        super(reactContext);
        this.reactContext = reactContext;
        reactContext.addLifecycleEventListener(this);
    }

    @Override
    public String getName() {
        return "Leveldown";
    }

    private static final String TAG = "ReactNativeLeveldown";

    private static final String E_ALREADY_OPEN = "E_ALREADY_OPEN";
    private static final String E_UNKNOWN_HANDLE = "E_UNKNOWN_HANDLE";
    private static final String E_OPEN_ERROR = "E_OPEN_ERROR";
    private static final String E_PUT_ERROR = "E_PUT_ERROR";
    private static final String E_DELETE_ERROR = "E_DELETE_ERROR";
    private static final String E_BATCH_UNKNOWN_OPERATION_TYPE = "E_BATCH_UNKNOWN_OPERATION_TYPE";
    private static final String E_BATCH_OPERATION_ERROR = "E_BATCH_OPERATION_ERROR";
    private static final String E_GET_ERROR = "E_GET_ERROR";
    private static final String E_CLEAR_ERROR = "E_CLEAR_ERROR";
    private static final String E_ITERATOR_CREATE = "E_ITERATOR_CREATE";
    private static final String E_ALREADY_INITIALIZED = "E_ALREADY_INITIALIZED";
    private static final String E_ITERATOR_GET = "E_ITERATOR_GET";
    private static final String E_ITERATOR_SEEK = "E_ITERATOR_SEEK";
    private static final String E_ITERATOR_CLOSE = "E_ITERATOR_SEEK";

    private Map<Integer, LevelDB> dbHandleTable = new HashMap<>();

    private LevelDB getDb(int dbHandle, Promise promise) {
        if (!dbHandleTable.containsKey(dbHandle)) {
            promise.reject(E_UNKNOWN_HANDLE,  String.format("Unknown DB handle %s", dbHandle));
        }
        return dbHandleTable.get(dbHandle);
    }
    private Map<Integer, LeveldownIterator> iteratorWrapperTable = new HashMap<>();

    private LeveldownIterator getIterator(int iteratorHandle, Promise promise) {
        if (!iteratorWrapperTable.containsKey(iteratorHandle)) {
            promise.reject(E_UNKNOWN_HANDLE,  String.format("Unknown iterator handle %s", iteratorHandle));
        }
        return iteratorWrapperTable.get(iteratorHandle);
    }

    @ReactMethod
    public void open(int dbHandle, String databaseName, boolean createIfMissing, boolean errorIfExists, Promise promise) {
        if (dbHandleTable.containsKey(dbHandle)) {
            promise.reject(E_ALREADY_OPEN,  String.format("DB with handle %s already open", dbHandle));
        }

        LevelDB.Configuration configuration = LevelDB.configure().createIfMissing(createIfMissing).exceptionIfExists(errorIfExists);

        try {
            LevelDB db = LevelDB.open(reactContext.getFilesDir().getAbsolutePath() +
                            File.separator + databaseName + ".db",
                    configuration);
            dbHandleTable.put(dbHandle, db);

            promise.resolve(null);
        } catch(LevelDBException e) {
            promise.reject(E_OPEN_ERROR, String.format("Error opening database %s: %s", databaseName, e.toString()));
        }
    }

    @ReactMethod
    public void put(int dbHandle, String key, String value, boolean sync, Promise promise) {
        LevelDB db = getDb(dbHandle, promise);

        try {
            db.put(LeveldownUtils.stringToByteArray(key), LeveldownUtils.stringToByteArray(value), sync);
            promise.resolve(null);
        } catch(LevelDBException e) {
            promise.reject(E_PUT_ERROR, String.format("Error writing %s: %s", key, e.toString()));
        }
    }

    @ReactMethod
    public void del(int dbHandle, String key, boolean sync, Promise promise) {
        LevelDB db = getDb(dbHandle, promise);

        try {
            db.del(LeveldownUtils.stringToByteArray(key), sync);
            promise.resolve(null);
        } catch(LevelDBException e) {
            promise.reject(E_DELETE_ERROR, String.format("Error deleting %s: %s", key, e.toString()));
        }
    }

    @ReactMethod
    public void batch(int dbHandle, ReadableArray operations, Promise promise) {
        LevelDB db = getDb(dbHandle, promise);

        try {
            WriteBatch batch = new SimpleWriteBatch();
            for (int i = 0; i < operations.size(); i++) {
                ReadableMap operation = operations.getMap(i);
                String type = operation.getString("type");
                String key = operation.getString("key");
                if (type.equals("put")) {
                    String value = operation.getString("value");
                    batch.put(LeveldownUtils.stringToByteArray(key), LeveldownUtils.stringToByteArray(value));
                } else if (type.equals("del")) {
                    batch.del(LeveldownUtils.stringToByteArray(key));
                } else {
                    promise.reject(E_BATCH_UNKNOWN_OPERATION_TYPE, String.format("Unknown operation type %s: %s", type, operation));
                }
            }

            db.write(batch);

            promise.resolve(null);
        } catch(LevelDBException e) {
            promise.reject(E_BATCH_OPERATION_ERROR, String.format("Error writing batch: %s", e.toString()));
        }
    }

    @ReactMethod
    public void get(int dbHandle, String key, Promise promise) {
        LevelDB db = getDb(dbHandle, promise);

        try {
            String output = LeveldownUtils.byteArrayToString(db.get(LeveldownUtils.stringToByteArray(key)));
            promise.resolve(output);
        } catch(LevelDBException e) {
            promise.reject(E_GET_ERROR, String.format("Error getting %s: %s", key, e.toString()));
        }
    }

    @ReactMethod
    public void close(int dbHandle, Promise promise) {
        LevelDB db = getDb(dbHandle, promise);

        db.close();
        dbHandleTable.remove(dbHandle);

        promise.resolve(null);
    }

    @ReactMethod
    public void clear(int dbHandle, ReadableMap options, Promise promise) {
        LevelDB db = getDb(dbHandle, promise);
        try {
            LeveldownIterator iterator = new LeveldownIterator(options, db);
            while (!iterator.isEnded()) {
                db.del(iterator.currentKey());
            }
        } catch(LevelDBException e) {
            promise.reject(E_CLEAR_ERROR, String.format("Error clearing: %s", e.toString()));
        }
        promise.resolve(null);
    }

    @ReactMethod
    public void createIterator(int dbHandle, int iteratorHandle, ReadableMap options, Promise promise) {
        if (iteratorWrapperTable.containsKey(iteratorHandle)) {
            promise.reject(E_ALREADY_INITIALIZED,  String.format("Already created iterator with handle %s", iteratorHandle));
        }
        LevelDB db = getDb(dbHandle, promise);

        try {
            LeveldownIterator iterator = new LeveldownIterator(options, db);
            iteratorWrapperTable.put(iteratorHandle, iterator);
        } catch(LevelDBException e) {
            promise.reject(E_ITERATOR_CREATE, String.format("Error creating iterator: %s", e.toString()));
        }
        promise.resolve(null);
    }

    @ReactMethod
    public void readIterator(int iteratorHandle, int count, Promise promise) {
        LeveldownIterator iterator = getIterator(iteratorHandle, promise);

        WritableArray keys = new WritableNativeArray();
        WritableArray values = new WritableNativeArray();

        int readIndex = 0;
        try {
            for (; readIndex < count && !iterator.isEnded(); iterator.advance(true), readIndex++) {
                byte[] key = iterator.currentKey();
                if (iterator.isReadsKeys()) {
                    keys.pushString(LeveldownUtils.byteArrayToString(key));
                }
                if (iterator.isReadsValues()) {
                    byte[] value = iterator.getDb().get(key, iterator.getSnapshot());
                    values.pushString(LeveldownUtils.byteArrayToString(value));
                }
            }
        } catch(LevelDBException e) {
            promise.reject(E_ITERATOR_GET, String.format("Error iterating: %s", e.toString()));
        }

        WritableMap output = new WritableNativeMap();
        output.putInt("readCount", readIndex);
        if (iterator.isReadsKeys()) {
            output.putArray("keys", keys);
        }
        if (iterator.isReadsValues()) {
            output.putArray("values", values);
        }
        promise.resolve(output);
    }

    @ReactMethod
    public void seekIterator(int iteratorHandle, String key, Promise promise) {
        LeveldownIterator iterator = getIterator(iteratorHandle, promise);

        try {
            byte[] keySlice = LeveldownUtils.stringToByteArray(key);
            iterator.getIterator().seek(keySlice);
            if (iterator.isReversed()) {
                if (!iterator.getIterator().isValid()) {
                    iterator.getIterator().seekToLast();
                } else if (Bytes.COMPARATOR.compare(iterator.currentKey(), keySlice) > 0) {
                    iterator.advance(false);
                }
            }
        } catch(LevelDBException e) {
            promise.reject(E_ITERATOR_SEEK, String.format("Error iterating: %s", e.toString()));
        }
        promise.resolve(null);
    }

    @ReactMethod
    public void endIterator(int iteratorHandle, Promise promise) {
        LeveldownIterator iterator = getIterator(iteratorHandle, promise);
        try {
            iterator.close();
        } catch(LevelDBException e) {
            promise.reject(E_ITERATOR_CLOSE, String.format("Error closing iterator: %s", e.toString()));
        }
        iteratorWrapperTable.remove(iteratorHandle);
        promise.resolve(null);
    }

    @Override
    public void onHostResume() {
        // nothing to do
    }

    @Override
    public void onHostPause() {
        // nothing to do
    }

    @Override
    public void onHostDestroy() {
        this.closeAll();
    }

    // This method is called when the "Reload" button is pressed in the dev menu.
    // We need to close all database handles and iterators, otherwise it is not
    // possible to open another database, as the database lock will still be
    // held by the process.
    @Override
    public void onCatalystInstanceDestroy() {
        this.closeAll();
    }

    private void closeAll() {
        for (LevelDB db : dbHandleTable.values()) {
            db.close();
        }
        try {
            for (LeveldownIterator iterator : iteratorWrapperTable.values()) {
                iterator.close();
            }
        } catch(LevelDBException e) {
            Log.e(TAG, String.format("Error closing iterators: %s", e.toString()));
        }
    }
}
