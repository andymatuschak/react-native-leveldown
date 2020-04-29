import * as ALD from "abstract-leveldown";
import supports from "level-supports";
import { NativeModules } from "react-native";

export interface ReactNativeLeveldownWriteOptions {
  sync?: boolean; // default false
}

class ReactNativeLeveldownIterator<K, V> extends ALD.AbstractIterator<K, V> {
  private static iteratorHandleCounter: number = 100;
  keyQueue: K[] | null;
  valueQueue: V[] | null;
  queueLength: number;
  isExhausted: boolean;
  iteratorHandle: number;

  constructor(db: ALD.AbstractLevelDOWN, dbHandle: number, options: ALD.AbstractIteratorOptions) {
    super(db);
    this.keyQueue = options.keys ? [] : null;
    this.valueQueue = options.values ? [] : null;
    this.queueLength = 0;
    this.isExhausted = false;
    this.iteratorHandle = ReactNativeLeveldownIterator.iteratorHandleCounter++;
    NativeModules.Leveldown.createIterator(dbHandle, this.iteratorHandle, options);
  }

  async _next(callback: ALD.ErrorKeyValueCallback<K | undefined, V | undefined>) {
    if (this.queueLength === 0 && !this.isExhausted) {
      // Fill the queue.
      try {
        const {keys, values, readCount} = await NativeModules.Leveldown.readIterator(this.iteratorHandle, 100);
        this.queueLength += readCount;
        this.isExhausted = readCount === 0;
        this.keyQueue = keys ?? null;
        this.valueQueue = values ?? null;
      } catch (error) {
        callback(error, undefined, undefined);
        return;
      }
    }

    if (this.isExhausted) {
      callback();
    } else {
      this.queueLength--;
      const key = this.keyQueue?.shift();
      const value = this.valueQueue?.shift();
      callback(undefined, key, value);
    }
  }

  _seek(target: string): void {
    NativeModules.Leveldown.seekIterator(this.iteratorHandle, target);
  }

  _end(callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.endIterator(this.iteratorHandle).then(callback(undefined)).catch(callback);
  }
}

export default class ReactNativeLeveldown extends ALD.AbstractLevelDOWN {
  private static dbHandleCounter: number = 1;
  private databaseName: string;
  private databaseHandle: number;

  constructor(databaseName: string) {
    super(
      supports({
        bufferKeys: false,
        snapshots: true,
        permanence: true,
        seek: true,
        clear: true,
        deferredOpen: false,
        openCallback: true,
        promises: true,
        createIfMissing: true,
        errorIfExists: true,
      }));
    this.databaseName = databaseName;
    this.databaseHandle = ReactNativeLeveldown.dbHandleCounter++;
  }

  _open(options: ALD.AbstractOpenOptions, callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.open(this.databaseHandle, this.databaseName, options.createIfMissing, options.errorIfExists)
      .then(() => callback(undefined)).catch(callback);
  }

  _put(key: string,
       value: string,
       options: ReactNativeLeveldownWriteOptions,
       callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.put(this.databaseHandle, key, value, options.sync ?? false)
      .then(() => callback(undefined)).catch(callback);
  }

  _get<V>(key: string, options: {}, callback: ALD.ErrorValueCallback<V>): void {
    NativeModules.Leveldown.get(this.databaseHandle, key).then((value: V) => callback(undefined, value)).catch(callback);
  }

  _del<V>(key: string,
          options: ReactNativeLeveldownWriteOptions,
          callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.del(this.databaseHandle, key, options.sync ?? false)
      .then(() => callback(undefined)).catch(callback);
  }

  _close(callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.close(this.databaseHandle).then(() => callback).catch(callback);
  }

  async _batch(operations: ReadonlyArray<ALD.AbstractBatch>,
               options: {},
               callback: ALD.ErrorCallback): Promise<void> {
    NativeModules.Leveldown.batch(this.databaseHandle, operations).then(() => callback(undefined)).catch(callback);
  }

  _iterator<K, V>(options: ALD.AbstractIteratorOptions): ReactNativeLeveldownIterator<K, V> {
    return new ReactNativeLeveldownIterator(this, this.databaseHandle, options);
  }
}
