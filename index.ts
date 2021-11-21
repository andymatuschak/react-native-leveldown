import * as ALD from "abstract-leveldown";
import { Buffer } from "buffer";
import { supports } from "level-supports";
import { NativeModules } from "react-native";

// @ts-ignore
const setImmediate = global.setImmediate;
const BinaryPrefix = "BINARY::";

export interface ReactNativeLeveldownWriteOptions {
  sync?: boolean; // default false
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
      let key: K;
      if (this.options.keys) {
        const keyString = this.keyQueue?.shift();
        const buffer = Buffer.from(keyString, "hex");
        key = (this.keyAsBuffer
          ? buffer
          : buffer.toString()) as K;
      }
      let value: V;
      if (this.options.values) {
        const valueString = this.valueQueue?.shift();
        let buffer;
        if (valueString.startsWith(BinaryPrefix)) {
          buffer = Buffer.from(valueString.slice(8), "base64");
        } else {
          buffer = Buffer.from(valueString);
        }
        value = (this.valueAsBuffer
          ? buffer
          : buffer.toString()) as V;
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
      target
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

  _serializeKey(key: any): string {
    if (typeof key === "string" || ArrayBuffer.isView(key)) {
      return Buffer.from(key as any).toString("hex");
    }
    return Buffer.from(key.toString()).toString("hex");
  }

  _serializeValue(value: any): string {
    if (typeof value === "string") {
      return value;
    }
    if (ArrayBuffer.isView(value)) {
      return `${BinaryPrefix}${Buffer.from(value as any).toString("base64")}`;
    }
    return value.toString();
  }

  _put(
    key: string | Buffer,
    value: string,
    options: ReactNativeLeveldownWriteOptions,
    callback: ALD.ErrorCallback
  ): void {
    NativeModules.Leveldown.put(
      this.databaseHandle,
      key,
      value,
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
    NativeModules.Leveldown.get(this.databaseHandle, key)
      .then((value: string) =>
        setImmediate(() => {
          let buffer;
          if (value.startsWith(BinaryPrefix)) {
            buffer = Buffer.from(value.slice(8), "base64");
          } else {
            buffer = Buffer.from(value);
          }
          const result = options.asBuffer ?? true ? buffer : buffer.toString();
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
      typeof key === "string" ? key : key.toString("hex"),
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
    NativeModules.Leveldown.batch(this.databaseHandle, operations)
      .then(() => setImmediate(callback as any))
      .catch(callback);
  }

  _iterator<K extends string | Buffer, V extends string | Buffer>(
    options: ALD.AbstractIteratorOptions
  ): ReactNativeLeveldownIterator<K, V> {
    return new ReactNativeLeveldownIterator(this, this.databaseHandle, options);
  }
}
