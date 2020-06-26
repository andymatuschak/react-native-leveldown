/**
 * Sample React Native App
 * https://github.com/facebook/react-native
 *
 * @format
 * @flow strict-local
 */

import React from 'react';
import {Text, SafeAreaView} from 'react-native';

import tape from 'tape';
import suite from 'abstract-leveldown/test';
import ReactNativeLeveldown from 'react-native-leveldown';

const App: () => React$Node = () => {
  const [logs, setLogs] = React.useState('Running tests...');
  React.useEffect(() => {
    global.__dirname = '<unknown dir name>';
    const test = tape.createHarness();

    let newLogs = '';
    let testCount = 0;
    let failedTests = [];
    test
      .createStream({objectMode: true})
      .on('data', function (row) {
        if (row.type === 'test') {
          console.log(`Running test '${row.name}'`);
        }
        if (row.type === 'assert') {
          testCount++;
          if (!row.ok) {
            failedTests.push(row);
          }
          const log = `${row.name || '(no name)'}: ${
            row.ok ? 'OK' : `FAILED (${JSON.stringify(row, null, '\t')})`
          }`;
          console.log(log);
          if (!row.ok) {
            newLogs += `${log}\n`;
          }
        }
      })
      .on('end', function (row) {
        newLogs += `Ran ${testCount} tests; ${failedTests.length} failed`;
        setLogs(newLogs);
      });

    const testCommon = suite.common({
      test,
      factory: function () {
        return new ReactNativeLeveldown(Math.random().toString());
      },
    });

    suite(testCommon);
  }, []);
  return (
    <SafeAreaView style={{flex: 1, overflow: "scroll"}}>
      <Text style={{flex: 1}}>{logs}</Text>
    </SafeAreaView>
  );
};

export default App;
