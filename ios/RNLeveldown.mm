#import "RNLeveldown.h"
#import "leveldb/db.h"
#import "leveldb/write_batch.h"

NSMapTable<NSNumber *, NSValue *> *_dbHandleTable;
NSMapTable<NSNumber *, NSValue *> *getDBHandleTable() {
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        _dbHandleTable = [[NSMapTable alloc] initWithKeyOptions:NSPointerFunctionsOpaqueMemory | NSPointerFunctionsIntegerPersonality valueOptions:NSPointerFunctionsOpaqueMemory | NSPointerFunctionsOpaquePersonality capacity:4];
    });
    return _dbHandleTable;
}

NSMutableDictionary<NSNumber *, NSValue *> *_iteratorWrapperTable;
NSMutableDictionary<NSNumber *, NSValue *> *getIteratorWrapperTable() {
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        _iteratorWrapperTable = [NSMutableDictionary dictionary];
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
NSValue *iteratorWrapperValuer = [iteratorWrapperTable objectForKey:@(iteratorHandle)]; \
if (!iteratorWrapperValuer) { \
reject(@"UnknownHandle", [NSString stringWithFormat:@"Unknown iterator handle %ld", iteratorHandle], nil); \
return; \
} \
RNLeveldownIterator iterator; \
[iteratorWrapperValuer getValue:&iterator];

#define GetSlice(input) leveldb::Slice([input UTF8String], [input lengthOfBytesUsingEncoding:NSUTF8StringEncoding])

struct RNLeveldownIterator {
    BOOL hasEndingBound;
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

// assumes !IsEnded();
void RNLeveldownIteratorAdvance(RNLeveldownIterator self) {
    self.isReversed ? self.iterator->Prev() : self.iterator->Next();
}

RNLeveldownIterator RNLeveldownIteratorInit(NSDictionary *options, leveldb::DB *db) {
    RNLeveldownIterator self;
    self.iterator = db->NewIterator(leveldb::ReadOptions());
    self.db = db;
    self.snapshot = self.db->GetSnapshot();

    self.limit = [options[@"limit"] integerValue];
    self.hasLimit = self.limit != -1;
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
        leveldb::Slice startingSlice = GetSlice(startingBound);
        self.iterator->Seek(startingSlice);
        if (!(self.isReversed ? upperBoundIsOpen : lowerBoundIsOpen) && self.iterator->Valid() && self.iterator->key().compare(startingSlice) == 0) {
            RNLeveldownIteratorAdvance(self);
        }
    } else {
        self.isReversed ? self.iterator->SeekToLast() : self.iterator->SeekToFirst();
    }

    NSString *endingBound = self.isReversed ? lowerBound : upperBound;
    if (endingBound) {
        size_t endingSliceLength = [endingBound lengthOfBytesUsingEncoding:[NSString defaultCStringEncoding]] + 1;
        self.endingSliceStorage = (char *)malloc(endingSliceLength);
        [endingBound getCString:self.endingSliceStorage maxLength:endingSliceLength encoding:[NSString defaultCStringEncoding]];
        self.endingSlice = leveldb::Slice(self.endingSliceStorage);
        self.endingBoundIsOpen = self.isReversed ? lowerBoundIsOpen : upperBoundIsOpen;
        self.hasEndingBound = YES;
    } else {
        self.hasEndingBound = NO;
        self.endingSliceStorage = NULL;
    }

    return self;
}

void RNLeveldownIteratorClose(RNLeveldownIterator self) {
    delete self.iterator;
    if (self.endingSliceStorage) {
        free(self.endingSliceStorage);
    }
    self.db->ReleaseSnapshot(self.snapshot);
}

inline BOOL RNLeveldownIteratorIsEnded(RNLeveldownIterator self) {
    if (!self.iterator->Valid()) {
        return YES;
    }

    if (self.hasLimit && self.stepCount >= self.limit) {
        return YES;
    }

    if (self.hasEndingBound) {
        auto comparison = self.iterator->key().compare(self.endingSlice);
        if ((comparison < 0 && self.isReversed) || (comparison > 0 && !self.isReversed) || (comparison == 0 && !self.endingBoundIsOpen)) {
            return YES;
        }
    }

    return NO;
}

// assumes !IsEnded();
inline leveldb::Slice RNLeveldownIteratorCurrentKey(RNLeveldownIterator self) {
    return self.iterator->key();
}

@implementation RNLeveldown

RCT_EXPORT_MODULE(Leveldown)

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
    NSLog(@"%ld Writing %ld ops", dbHandle, [operations count]);

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
    NSLog(@"%ld Wrote %ld ops", dbHandle, [operations count]);
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
    [dbHandleTable removeObjectForKey:@(dbHandle)];
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
    [iteratorWrapperTable setObject:[NSValue value:&iteratorWrapper withObjCType:@encode(RNLeveldownIterator)] forKey:@(iteratorHandle)];
    resolve(nil);
}

RCT_REMAP_METHOD(readIterator, readIterator:(NSInteger)iteratorHandle count:(NSInteger)count resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
    GetIterator(iteratorHandle);
    NSMutableArray *keys = nil;
    if (iterator.readsKeys) {
        keys = [NSMutableArray arrayWithCapacity:count];
    }
    NSMutableArray *values = nil;
    if (iterator.readsValues) {
        values = [NSMutableArray arrayWithCapacity:count];
    }
    leveldb::ReadOptions readOptions;
    readOptions.snapshot = iterator.snapshot;

    NSInteger readIndex = 0;
    for (readIndex = 0; readIndex < count && !RNLeveldownIteratorIsEnded(iterator); RNLeveldownIteratorAdvance(iterator), readIndex++) {
        leveldb::Slice key = RNLeveldownIteratorCurrentKey(iterator);
        if (iterator.readsKeys) {
            [keys addObject:NSStringFromCPPString(key.ToString())];
        }
        if (iterator.readsValues) {
            std::string value;
            leveldb::Status status = iterator.db->Get(readOptions, key, &value);
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
    iterator.iterator->Seek(GetSlice(key));
    resolve(nil);
}

RCT_REMAP_METHOD(endIterator, endIterator:(NSInteger)iteratorHandle resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
    GetIterator(iteratorHandle);
    RNLeveldownIteratorClose(iterator);
    [iteratorWrapperTable removeObjectForKey:@(iteratorHandle)];
    resolve(nil);
}

- (dispatch_queue_t)methodQueue {
    return dispatch_queue_create("RNLeveldown", NULL);
}


@end
