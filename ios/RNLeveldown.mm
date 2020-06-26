#import "RNLeveldown.h"
#import "leveldb/db.h"
#import "leveldb/write_batch.h"

struct RNLeveldownIterator {
    leveldb::Slice startingSlice;
    char *startingSliceStorage;
    BOOL startingBoundIsOpen;

    leveldb::Slice endingSlice;
    char *endingSliceStorage;
    BOOL endingBoundIsOpen;

    BOOL isReversed;

    BOOL hasLimit;
    NSInteger limit;
    NSInteger stepCount;

    BOOL readsKeys;
    BOOL readsValues;

    leveldb::DB *db;
    const leveldb::Snapshot *snapshot;
    leveldb::Iterator *iterator;
    NSInteger dbHandle;
};

NSMapTable *_dbHandleTable;
NSMapTable *getDBHandleTable() {
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        _dbHandleTable = [[NSMapTable alloc] initWithKeyOptions:NSPointerFunctionsOpaqueMemory | NSPointerFunctionsIntegerPersonality valueOptions:NSPointerFunctionsOpaqueMemory | NSPointerFunctionsOpaquePersonality capacity:4];
    });
    return _dbHandleTable;
}

NSUInteger getIteratorSize(const void * _Nonnull value) { return sizeof(RNLeveldownIterator); }

NSMapTable *_iteratorWrapperTable;
NSMapTable *getIteratorWrapperTable() {
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        NSPointerFunctions *keyFunctions = [NSPointerFunctions pointerFunctionsWithOptions:NSPointerFunctionsOpaqueMemory | NSPointerFunctionsIntegerPersonality];
        NSPointerFunctions *valueFunctions = [NSPointerFunctions pointerFunctionsWithOptions:NSPointerFunctionsMallocMemory | NSPointerFunctionsStructPersonality | NSPointerFunctionsCopyIn];
        valueFunctions.sizeFunction = getIteratorSize;
        _iteratorWrapperTable = [[NSMapTable alloc] initWithKeyPointerFunctions:keyFunctions valuePointerFunctions:valueFunctions capacity:4];
    });
    return _iteratorWrapperTable;
}

#define NSStringFromCPPString(cppString) [NSString stringWithCString:cppString.c_str() encoding:NSUTF8StringEncoding]

#define GetDB(dbHandle) \
auto dbHandleTable = getDBHandleTable(); \
leveldb::DB *db = (leveldb::DB *)NSMapGet(dbHandleTable, (void *)dbHandle); \
if (!db) { \
reject(@"UnknownHandle", [NSString stringWithFormat:@"Unknown DB handle %ld", dbHandle], nil); \
return; \
} \

#define GetIterator(iteratorHandle) \
auto iteratorWrapperTable = getIteratorWrapperTable(); \
RNLeveldownIterator *iterator = (RNLeveldownIterator *)NSMapGet(iteratorWrapperTable, (void *)iteratorHandle); \
if (!iterator) { \
reject(@"UnknownHandle", [NSString stringWithFormat:@"Unknown iterator handle %ld", iteratorHandle], nil); \
return; \
} \

#define GetSlice(input) leveldb::Slice([input UTF8String], [input lengthOfBytesUsingEncoding:NSUTF8StringEncoding])

// assumes !IsEnded();
void RNLeveldownIteratorAdvance(RNLeveldownIterator *self, BOOL increaseStepCount) {
    if (increaseStepCount) {
        self->stepCount++;
    }
    self->isReversed ? self->iterator->Prev() : self->iterator->Next();
}

RNLeveldownIterator RNLeveldownIteratorInit(NSDictionary *options, leveldb::DB *db) {
    RNLeveldownIterator self;
    self.iterator = db->NewIterator(leveldb::ReadOptions());
    self.db = db;
    self.snapshot = self.db->GetSnapshot();

    self.limit = [options[@"limit"] integerValue];
    self.hasLimit = [options objectForKey:@"limit"] != nil && self.limit != -1;
    self.isReversed = [options[@"reverse"] boolValue];

    self.readsKeys = [options[@"keys"] boolValue];
    self.readsValues = [options[@"values"] boolValue];

    NSString *lowerBound = nil;
    BOOL lowerBoundIsOpen = YES;
    if (options[@"gt"]) {
        lowerBound = options[@"gt"];
        lowerBoundIsOpen = NO;
    } else if (options[@"gte"]) {
        lowerBound = options[@"gte"];
        lowerBoundIsOpen = YES;
    }

    NSString *upperBound = nil;
    BOOL upperBoundIsOpen = YES;
    if (options[@"lt"]) {
        upperBound = options[@"lt"];
        upperBoundIsOpen = NO;
    } else if (options[@"lte"]) {
        upperBound = options[@"lte"];
        upperBoundIsOpen = YES;
    }

    NSString *startingBound = self.isReversed ? upperBound : lowerBound;
    if (startingBound) {
        size_t startingSliceLength = [startingBound lengthOfBytesUsingEncoding:[NSString defaultCStringEncoding]] + 1;
        self.startingSliceStorage = (char *)malloc(startingSliceLength);
        [startingBound getCString:self.startingSliceStorage maxLength:startingSliceLength encoding:[NSString defaultCStringEncoding]];
        self.startingSlice = leveldb::Slice(self.startingSliceStorage);
        self.startingBoundIsOpen = self.isReversed ? upperBoundIsOpen : lowerBoundIsOpen;

        self.iterator->Seek(self.startingSlice);
        if (self.iterator->Valid()) {
            auto comparison = self.iterator->key().compare(self.startingSlice);
            if ((!(self.isReversed ? upperBoundIsOpen : lowerBoundIsOpen) && comparison == 0) || (self.isReversed && comparison > 0)) {
                RNLeveldownIteratorAdvance(&self, false);
            }
        } else if (self.isReversed) {
            // We must have seeked past the end.
            self.iterator->SeekToLast();
        }
    } else {
        self.startingSliceStorage = NULL;
        self.isReversed ? self.iterator->SeekToLast() : self.iterator->SeekToFirst();
    }

    NSString *endingBound = self.isReversed ? lowerBound : upperBound;
    if (endingBound) {
        size_t endingSliceLength = [endingBound lengthOfBytesUsingEncoding:[NSString defaultCStringEncoding]] + 1;
        self.endingSliceStorage = (char *)malloc(endingSliceLength);
        [endingBound getCString:self.endingSliceStorage maxLength:endingSliceLength encoding:[NSString defaultCStringEncoding]];
        self.endingSlice = leveldb::Slice(self.endingSliceStorage);
        self.endingBoundIsOpen = self.isReversed ? lowerBoundIsOpen : upperBoundIsOpen;
    } else {
        self.endingSliceStorage = NULL;
    }

    return self;
}

void RNLeveldownIteratorClose(RNLeveldownIterator &self) {
    delete self.iterator;
    if (self.endingSliceStorage) {
        free(self.endingSliceStorage);
    }
    if (self.startingSliceStorage) {
        free(self.startingSliceStorage);
    }
    self.db->ReleaseSnapshot(self.snapshot);
}

inline BOOL RNLeveldownIteratorIsEnded(RNLeveldownIterator &self) {
    if (!self.iterator->Valid()) {
        return YES;
    }

    if (self.hasLimit && self.stepCount >= self.limit) {
        return YES;
    }

    if (self.endingSliceStorage) {
        auto comparison = self.iterator->key().compare(self.endingSlice);
        if ((comparison < 0 && self.isReversed) || (comparison > 0 && !self.isReversed) || (comparison == 0 && !self.endingBoundIsOpen)) {
            return YES;
        }
    }
    if (self.startingSliceStorage) {
        auto comparison = self.iterator->key().compare(self.startingSlice);
        if ((comparison > 0 && self.isReversed) || (comparison < 0 && !self.isReversed) || (comparison == 0 && !self.startingBoundIsOpen)) {
            return YES;
        }
    }

    return NO;
}

// assumes !IsEnded();
inline leveldb::Slice RNLeveldownIteratorCurrentKey(RNLeveldownIterator &self) {
    return self.iterator->key();
}

@implementation RNLeveldown

RCT_EXPORT_MODULE(Leveldown)

- (void)dealloc {
    NSMapTable *dbHandleTable = getDBHandleTable();
    NSMapEnumerator dbHandleEnumerator = NSEnumerateMapTable(dbHandleTable);
    NSInteger dbHandle;
    leveldb::DB *db;
    while (NSNextMapEnumeratorPair(&dbHandleEnumerator, (void **)&dbHandle, (void **)&db)) {
        delete db;
    }
    NSResetMapTable(dbHandleTable);
    NSEndMapTableEnumeration(&dbHandleEnumerator);

    NSMapTable *iteratorWrapperTable = getIteratorWrapperTable();
    NSMapEnumerator iteratorWrapperEnumerator = NSEnumerateMapTable(iteratorWrapperTable);
    NSInteger iteratorWrapperHandle;
    RNLeveldownIterator iterator;
    while (NSNextMapEnumeratorPair(&iteratorWrapperEnumerator, (void **)&iteratorWrapperHandle, (void **)&iterator)) {
        RNLeveldownIteratorClose(iterator);
    }
    NSResetMapTable(iteratorWrapperTable);
    NSEndMapTableEnumeration(&iteratorWrapperEnumerator);
}

RCT_REMAP_METHOD(open, openDB:(NSInteger)dbHandle databaseName:(NSString *)databaseName createIfMissing:(BOOL)createIfMissing errorIfExists:(BOOL)errorIfExists  resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
    auto dbHandleTable = getDBHandleTable();
    if ([dbHandleTable objectForKey:@(dbHandle)]) {
        reject(@"AlreadyOpen", [NSString stringWithFormat:@"DB with handle %ld already open", dbHandle], nil);
        return;
    }

    leveldb::Options openOptions;
    openOptions.create_if_missing = createIfMissing;
    openOptions.error_if_exists = errorIfExists;


    NSURL *documentFolderURL = [[NSFileManager defaultManager] URLsForDirectory:NSDocumentDirectory inDomains:NSUserDomainMask][0];

    leveldb::DB *db;
    leveldb::Status status = leveldb::DB::Open(openOptions, std::string([[NSString stringWithFormat:@"%@/%@.db", [documentFolderURL path], databaseName] UTF8String]), &db);
    if (status.ok()) {
        NSMapInsert(dbHandleTable, (void *)dbHandle, db);
        resolve(nil);
    } else {
        reject(@"OpenError", [NSString stringWithFormat:@"error opening database %@: %@", databaseName, NSStringFromCPPString(status.ToString())], nil);
    }
}

RCT_REMAP_METHOD(put, putDB:(NSInteger)dbHandle key:(NSString *)key value:(NSString *)value sync:(BOOL)sync resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
    GetDB(dbHandle);
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync;

    leveldb::Status status = db->Put(writeOptions, GetSlice(key), GetSlice(value));
    if (status.ok()) {
        resolve(nil);
    } else {
        reject(@"PutError", [NSString stringWithFormat:@"error writing %@: %@", key, NSStringFromCPPString(status.ToString())], nil);
    }
}

RCT_REMAP_METHOD(del, delDB:(NSInteger)dbHandle key:(NSString *)key sync:(BOOL)sync resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
    GetDB(dbHandle);
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync;

    leveldb::Status status = db->Delete(writeOptions, GetSlice(key));
    if (status.ok()) {
        resolve(nil);
    } else {
        reject(@"DeleteError", [NSString stringWithFormat:@"error deleting %@: %@", key, NSStringFromCPPString(status.ToString())], nil);
    }
}

RCT_REMAP_METHOD(batch, batchDB:(NSInteger)dbHandle operations:(NSArray *)operations resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
  GetDB(dbHandle);

    leveldb::WriteBatch batch;
  for (NSDictionary *operation in operations) {
    NSString *type = operation[@"type"];
    NSString *key = operation[@"key"];
    if ([type isEqualToString:@"put"]) {
      NSString *value = operation[@"value"];
        batch.Put(GetSlice(key), GetSlice(value));
    } else if ([type isEqualToString:@"del"]) {
        batch.Delete(GetSlice(key));
    } else {
      reject(@"BatchUnknownOperationType", [NSString stringWithFormat:@"Unknown operation type %@: %@", type, operation], nil);
    }
  }
    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
      reject(@"BatchOperationError", [NSString stringWithFormat:@"Error writing batch: %@", NSStringFromCPPString(status.ToString())], nil);
      return;
    }
  resolve(nil);
}


RCT_REMAP_METHOD(get, getDB:(NSInteger)dbHandle key:(NSString *)key resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
    GetDB(dbHandle);

    std::string output;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), GetSlice(key), &output);
    if (status.ok()) {
        resolve(NSStringFromCPPString(output));
    } else {
        reject(@"GetError", [NSString stringWithFormat:@"error getting %@: %@", key, NSStringFromCPPString(status.ToString())], nil);
    }
}

RCT_REMAP_METHOD(close, closeDB:(NSInteger)dbHandle resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
    GetDB(dbHandle);
    delete db;
    NSMapRemove(dbHandleTable, (void *)dbHandle);

    resolve(nil);
}



RCT_REMAP_METHOD(clear, clearDB:(NSInteger)dbHandle options:(NSDictionary *)options resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject)
{
    GetDB(dbHandle);
    RNLeveldownIterator iterator = RNLeveldownIteratorInit(options, db);
    while (!RNLeveldownIteratorIsEnded(iterator)) {
        leveldb::Status status = db->Delete(leveldb::WriteOptions(), RNLeveldownIteratorCurrentKey(iterator));
        if (!status.ok()) {
            reject(@"ClearError", [NSString stringWithFormat:@"error clearing: %@", NSStringFromCPPString(status.ToString())], nil);
        }
    }
    resolve(nil);
}

RCT_REMAP_METHOD(createIterator, createIteratorDB:(NSInteger)dbHandle iteratorHandle:(NSInteger)iteratorHandle options:(NSDictionary *)options resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
    auto iteratorWrapperTable = getIteratorWrapperTable();
    if ([iteratorWrapperTable objectForKey:@(iteratorHandle)]) {
        reject(@"AlreadyInitialized", [NSString stringWithFormat:@"Already created iterator with handle %ld", dbHandle], nil);
        return;
    }
    GetDB(dbHandle);

    RNLeveldownIterator iteratorWrapper = RNLeveldownIteratorInit(options, db);
    NSMapInsert(iteratorWrapperTable, (void *)iteratorHandle, &iteratorWrapper);
    resolve(nil);
}

RCT_REMAP_METHOD(readIterator, readIterator:(NSInteger)iteratorHandle count:(NSInteger)count resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
    GetIterator(iteratorHandle);
    NSMutableArray *keys = nil;
    if (iterator->readsKeys) {
        keys = [NSMutableArray arrayWithCapacity:count];
    }
    NSMutableArray *values = nil;
    if (iterator->readsValues) {
        values = [NSMutableArray arrayWithCapacity:count];
    }
    leveldb::ReadOptions readOptions;
    readOptions.snapshot = iterator->snapshot;

    NSInteger readIndex = 0;
    for (readIndex = 0; readIndex < count && !RNLeveldownIteratorIsEnded(*iterator); RNLeveldownIteratorAdvance(iterator, true), readIndex++) {
        leveldb::Slice key = RNLeveldownIteratorCurrentKey(*iterator);
        if (iterator->readsKeys) {
            [keys addObject:NSStringFromCPPString(key.ToString())];
        }
        if (iterator->readsValues) {
            std::string value;
            leveldb::Status status = iterator->db->Get(readOptions, key, &value);
            if (status.ok()) {
                [values addObject:NSStringFromCPPString(value)];
            } else {
                reject(@"IteratorGetError", [NSString stringWithFormat:@"error iterating: %@", NSStringFromCPPString(status.ToString())], nil);
            }
        }
    }

    NSMutableDictionary *output = [NSMutableDictionary dictionaryWithCapacity:3];
    [output setObject:@(readIndex) forKey:@"readCount"];
    if (keys) {
        [output setObject:keys forKey:@"keys"];
    }
    if (values) {
        [output setObject:values forKey:@"values"];
    }
    resolve(output);
}

RCT_REMAP_METHOD(seekIterator, seekIterator:(NSInteger)iteratorHandle key:(NSString *)key resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
    GetIterator(iteratorHandle);
    auto keySlice = GetSlice(key);
    iterator->iterator->Seek(keySlice);
    if (iterator->isReversed) {
        if (!iterator->iterator->Valid()) {
            // We must have seeked past the end.
            iterator->iterator->SeekToLast();
        } else if (iterator->iterator->key().compare(keySlice) > 0) {
            // We seeked past the target; step back.
            RNLeveldownIteratorAdvance(iterator, false);
        }
    }
    resolve(nil);
}

RCT_REMAP_METHOD(endIterator, endIterator:(NSInteger)iteratorHandle resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
    GetIterator(iteratorHandle);
    RNLeveldownIteratorClose(*iterator);
    NSMapRemove(iteratorWrapperTable, (void *)iteratorHandle);
    resolve(nil);
}

- (dispatch_queue_t)methodQueue {
    return dispatch_queue_create("RNLeveldown", NULL);
}


@end
