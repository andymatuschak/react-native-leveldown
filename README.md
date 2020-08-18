# react-native-leveldown

This library implements an [`abstract-leveldown`](https://github.com/Level/abstract-leveldown) compatible interface to [LevelDB](https://github.com/google/leveldb) for [React Native](https://reactnative.dev). The implementation is a thin [Native Module](https://reactnative.dev/docs/native-modules-setup) binding directly to the original C++ implementation of LevelDB.

The native bindings are implemented for iOS and Android. The iOS bindings will also run on macOS via [Catalyst](https://developer.apple.com/mac-catalyst/).

My thanks to Giacomo Randazzo (@RAN3000) for contributing the Android bindings to this package.

## Usage

Install via `yarn add react-native-leveldown`. Make sure to `cd ios; pod install` to build the native module for iOS.

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

Note that databases are stored in the [app container's Documents directory on iOS](https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/FileSystemProgrammingGuide/FileSystemOverview/FileSystemOverview.html) and the [app's persistent files directory on Android](https://developer.android.com/training/data-storage/app-specific#internal-access-files). In the future, the constructor API should probably be extended to add an option to store it instead in some semi-durable cache location, or an ephemeral temporary directory. 

## Testing

This library passes the `abstract-leveldown` test suite. To run the tests:

```
# Setup:
cd testapp
yarn install

# To run the tests on iOS:
(cd ios; pod install)
yarn ios

# To run the tests on Android:
yarn android
```

## Alternatives

If you're looking for an implementation with no native code, you may be interested in [`asyncstorage-down`](https://github.com/tradle/asyncstorage-down), which implements `abstract-leveldown` using React Native's built-in `AsyncStorage`. Note that this approach will be much slower than native LevelDB bindings, particularly on iOS, where `AsyncStorage` is basically a serialized JSON blob.
