package com.reactnativeleveldown;

import com.facebook.react.bridge.ReadableMap;
import com.github.hf.leveldb.Iterator;
import com.github.hf.leveldb.LevelDB;
import com.github.hf.leveldb.Snapshot;
import com.github.hf.leveldb.exception.LevelDBClosedException;
import com.github.hf.leveldb.util.Bytes;


class LeveldownIterator {

    private byte[] startingSlice;
    private boolean startingBoundIsOpen;

    private byte[] endingSlice;
    private boolean endingBoundIsOpen;

    private boolean isReversed;

    private boolean hasLimit;
    private int limit;
    private int stepCount;

    private boolean readsKeys;
    private boolean readsValues;

    private LevelDB db;
    private Snapshot snapshot;
    private Iterator iterator;

    public LeveldownIterator(ReadableMap options, LevelDB db) throws LevelDBClosedException {
        this.iterator = db.iterator();
        this.db = db;
        this.snapshot = db.obtainSnapshot();


        this.limit = options.getInt("limit");
        this.stepCount = 0;
        this.hasLimit = options.hasKey("limit") && limit != -1;
        this.isReversed = options.getBoolean("reverse");

        this.readsKeys = options.getBoolean("keys");
        this.readsValues = options.getBoolean("values");

        String lowerBound = null;
        boolean lowerBoundIsOpen = true;
        if (options.hasKey("gt")) {
            lowerBound = options.getString("gt");
            lowerBoundIsOpen = false;
        } else if (options.hasKey("gte")) {
            lowerBound = options.getString("gte");
        }

        String upperBound = null;
        boolean upperBoundIsOpen = true;
        if (options.hasKey("lt")) {
            upperBound = options.getString("lt");
            upperBoundIsOpen = false;
        } else if (options.hasKey("lte")) {
            upperBound = options.getString("lte");
        }

        String startingBound = isReversed ? upperBound : lowerBound;
        if (startingBound != null) {
            this.startingSlice = LeveldownUtils.stringToByteArray(startingBound);
            this.startingBoundIsOpen = isReversed ? upperBoundIsOpen : lowerBoundIsOpen;

            iterator.seek(startingSlice);
            if (iterator.isValid()) {
                int comparison = Bytes.COMPARATOR.compare(iterator.key(), startingSlice);
                if ((!startingBoundIsOpen && comparison == 0) || (isReversed && comparison > 0)) {
                    advance(false);
                }
            } else if (isReversed) {
                iterator.seekToLast();
            }
        } else {
            if (isReversed) {
                iterator.seekToLast();
            } else {
                iterator.seekToFirst();
            }
        }

        String endingBound = isReversed ? lowerBound : upperBound;
        if (endingBound != null) {
            this.endingSlice = LeveldownUtils.stringToByteArray(endingBound);
            this.endingBoundIsOpen = isReversed ? lowerBoundIsOpen : upperBoundIsOpen;
        }
    }

    public void close() throws LevelDBClosedException {
        iterator.close();
        db.releaseSnapshot(snapshot);
    }

    public void advance(boolean increaseStepCount) throws LevelDBClosedException {
        if (increaseStepCount) {
            stepCount++;
        }
        if (isReversed) {
            iterator.previous();
        } else {
            iterator.next();
        }
    }

    public boolean isEnded() throws LevelDBClosedException {
        if (!iterator.isValid()) {
            return true;
        }

        if (hasLimit && stepCount >= limit) {
            return true;
        }

        if (endingSlice != null) {
            int comparison = Bytes.COMPARATOR.compare(iterator.key(), endingSlice);
            if ((comparison < 0 && isReversed) || (comparison > 0 && !isReversed) || (comparison == 0 && !endingBoundIsOpen)) {
                return true;
            }
        }

        if (startingSlice != null) {
            int comparison = Bytes.COMPARATOR.compare(iterator.key(), startingSlice);
            if ((comparison > 0 && isReversed) || (comparison < 0 && !isReversed) || (comparison == 0 && !startingBoundIsOpen)) {
                return true;
            }
        }

        return false;
    }

    public byte[] currentKey() throws LevelDBClosedException {
        return iterator.key();
    }

    public Iterator getIterator() {
        return iterator;
    }

    public boolean isReversed() {
        return isReversed;
    }

    public boolean isReadsKeys() {
        return readsKeys;
    }

    public boolean isReadsValues() {
        return readsValues;
    }

    public LevelDB getDb() {
        return db;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

}
