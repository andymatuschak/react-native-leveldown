import * as ALD from "abstract-leveldown";
import { Buffer } from "buffer";
import { supports } from "level-supports";
import { NativeModules } from "react-native";

// @ts-ignore
const setImmediate = global.setImmediate;

export interface ReactNativeLeveldownWriteOptions {
  sync?: boolean; // default false
}

function inputAsString(key: any): string {
  if (typeof key === "string") {
    return key;
  } else if (Buffer.isBuffer(key)) {
    return key.toString("binary");
  } else {
    return key.toString();
  }
}

class ReactNativeLeveldownIterator<
  K extends string | Buffer,
  V extends string | Buffer
> extends ALD.AbstractIterator<K, V> {
  private static iteratorHandleCounter: number = 100;
  keyQueue: string[] | null;
  valueQueue: string[] | null;
  queueLength: number;
  isExhausted: boolean;
  iteratorHandle: number;
  isInImmediate: boolean;
  keyAsBuffer: boolean;
  valueAsBuffer: boolean;
  options: ALD.AbstractIteratorOptions;

  constructor(
    db: ALD.AbstractLevelDOWN,
    dbHandle: number,
    options: ALD.AbstractIteratorOptions
  ) {
    super(db);
    this.keyQueue = options.keys ? [] : null;
    this.valueQueue = options.values ? [] : null;
    this.queueLength = 0;
    this.isExhausted = false;
    this.iteratorHandle = ReactNativeLeveldownIterator.iteratorHandleCounter++;
    this.isInImmediate = false;
    this.keyAsBuffer = options.keyAsBuffer ?? true;
    this.valueAsBuffer = options.valueAsBuffer ?? true;
    NativeModules.Leveldown.createIterator(dbHandle, this.iteratorHandle, {
      ...options,
      gte: options.gte ?? (options.reverse ? options.end : options.start),
      lte: options.lte ?? (options.reverse ? options.start : options.end),
    });
  this.options = options;
  }

  async _next(
    callback: ALD.ErrorKeyValueCallback<K | undefined, V | undefined>
  ) {
    if (this.queueLength === 0 && !this.isExhausted) {
      // Fill the queue.
      try {
        const {
          keys,
          values,
          readCount,
        } = await NativeModules.Leveldown.readIterator(
          this.iteratorHandle,
          100
        );
        this.queueLength += readCount;
        this.isExhausted = readCount === 0;
        this.keyQueue = this.options.keys ? keys ?? null : null;
        this.valueQueue = this.options.values ? values ?? null : null;
      } catch (error) {
        setImmediate(() => callback(error, undefined, undefined));
        return;
      }
    }

    if (this.isExhausted) {
      setImmediate(callback as any);
    } else {
      this.queueLength--;
      let keyString: string;
      let key: K;
      if (this.options.keys) {
        keyString = this.keyQueue?.shift();
        key = (this.keyAsBuffer
          ? Buffer.from(keyString, "binary")
          : keyString) as K;
      }
      let valueString: string;
      let value: V;
      if (this.options.values) {
        valueString = this.valueQueue?.shift();
        value = (this.valueAsBuffer
          ? Buffer.from(valueString, "binary")
          : valueString) as V;
      }
      if (this.isInImmediate) {
        callback(undefined, key, value);
      } else {
        setImmediate(() => {
          this.isInImmediate = true;
          callback(undefined, key, value);
          this.isInImmediate = false;
        });
      }
    }
  }

  _seek(target: string | Buffer): void {
    this.keyQueue = [];
    this.valueQueue = [];
    this.queueLength = 0;
    this.isExhausted = false;
    NativeModules.Leveldown.seekIterator(
      this.iteratorHandle,
      inputAsString(target)
    );
  }

  _end(callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.endIterator(this.iteratorHandle)
      .then(() => setImmediate(callback as any))
      .catch(callback);
  }
}

export default class ReactNativeLeveldown extends ALD.AbstractLevelDOWN {
  private static dbHandleCounter: number = 1;
  private databaseName: string;
  private databaseHandle: number;

  constructor(databaseName: string) {
    super(
      // @ts-ignore
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
      })
    );
    this.databaseName = databaseName;
    this.databaseHandle = ReactNativeLeveldown.dbHandleCounter++;
  }

  _open(options: ALD.AbstractOpenOptions, callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.open(
      this.databaseHandle,
      this.databaseName,
      options.createIfMissing,
      options.errorIfExists
    )
      .then(() => setImmediate(() => callback(undefined)))
      .catch(callback);
  }

  _put(
    key: string | Buffer,
    value: string,
    options: ReactNativeLeveldownWriteOptions,
    callback: ALD.ErrorCallback
  ): void {
    NativeModules.Leveldown.put(
      this.databaseHandle,
      inputAsString(key),
      inputAsString(value),
      options.sync ?? false
    )
      .then(() => setImmediate(callback as any))
      .catch(callback);
  }

  _get<V extends string | Buffer>(
    key: string | Buffer,
    options: { asBuffer: boolean },
    callback: ALD.ErrorValueCallback<V>
  ): void {
    NativeModules.Leveldown.get(this.databaseHandle, inputAsString(key))
      .then((value: string) =>
        setImmediate(() => {
          const result = options.asBuffer ?? true ? Buffer.from(value) : value;
          return callback(undefined, result as V);
        })
      )
      .catch(callback);
  }

  _del<V>(
    key: string | Buffer,
    options: ReactNativeLeveldownWriteOptions,
    callback: ALD.ErrorCallback
  ): void {
    NativeModules.Leveldown.del(
      this.databaseHandle,
      inputAsString(key),
      options.sync ?? false
    )
      .then(() => setImmediate(callback as any))
      .catch(callback);
  }

  _close(callback: ALD.ErrorCallback): void {
    NativeModules.Leveldown.close(this.databaseHandle)
      .then(() => setImmediate(callback as any))
      .catch(callback);
  }

  async _batch(
    operations: ReadonlyArray<ALD.AbstractBatch>,
    options: {},
    callback: ALD.ErrorCallback
  ): Promise<void> {
    NativeModules.Leveldown.batch(this.databaseHandle, operations.map((op) => {
      const newOP = {...op};
      newOP.key = inputAsString(newOP.key);
      if (newOP.type === "put") {
        newOP.value = inputAsString(newOP.value);
      }
      return newOP;
    }))
      .then(() => setImmediate(callback as any))
      .catch(callback);
  }

  _iterator<K extends string | Buffer, V extends string | Buffer>(
    options: ALD.AbstractIteratorOptions
  ): ReactNativeLeveldownIterator<K, V> {
    return new ReactNativeLeveldownIterator(this, this.databaseHandle, options);
  }
}
