# react-native-leveldown

This library implements an [`abstract-leveldown`](https://github.com/Level/abstract-leveldown) compatible interface to [LevelDB](https://github.com/google/leveldb) for [React Native](https://reactnative.dev). The implementation is a thin [Native Module](https://reactnative.dev/docs/native-modules-setup) binding directly to the original C++ implementation of LevelDB.

The native bindings are currently implemented only for iOS (and macOS via Catalyst). [A patch implementing Android bindings would be welcome](https://github.com/andymatuschak/react-native-leveldown/issues/1)! In the meantime, you can get a compatible interface via [level-js](https://github.com/Level/level-js) on top of [indexeddbshim](https://github.com/indexeddbshim/indexeddbshim) on top of [expo-sqlite](https://github.com/expo/expo/tree/master/packages/expo-sqlite), but that's obviously much much slower.

You may also be interested in [`asyncstorage-down`](https://github.com/tradle/asyncstorage-down), another `abstract-leveldown` implementation for React Native. It includes no native code (which may be preferable in your configuration) by using `AsyncStorage` as its native storage layer. This approach will be much slower than native LevelDB bindings, particularly on iOS, where `AsyncStorage` is basically a serialized JSON blob.

## Usage

Install via `yarn add react-native-leveldown`. Make sure to `cd ios; pod install` to build the native module.

This module implements the interface defined in [`abstract-leveldown`](https://github.com/Level/abstract-leveldown), so you can either use it directly as documented there or somewhat more conveniently using the [`levelup`](https://github.com/Level/levelup) wrapper.

Typical usage:

```
import RNLeveldown from "react-native-leveldown";
import LevelUp from "levelup";

const db = LevelUp(new RNLeveldown("myDatabaseName"));
await db.put("hello", "world");
await db.get("hello") // # => "world"
await db.close();
```

Note that databases are stored in the app container's Documents directory. In the future, the constructor API should probably be extended to add an option to store it instead in some semi-durable cache location, or an ephemeral temporary directory. 

## Testing

This library passes the `abstract-leveldown` test suite. To run the tests, launch the React Native app in the `testapp` subdirectory.
