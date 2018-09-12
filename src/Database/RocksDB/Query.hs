{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Database.RocksDB.Query where

import qualified Data.ByteString          as B
import           Conduit
import           Data.Serialize           as S
import           Database.RocksDB         as R
import           UnliftIO

class Key key
class KeyValue key value

-- | Read a value from the database, or 'Nothing' if not found.
retrieve ::
       (MonadIO m, KeyValue key value, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> key
    -> m (Maybe value)
retrieve db snapshot key = do
    let opts = defaultReadOptions {useSnapshot = snapshot}
    R.get db opts (encode key) >>= \case
        Nothing -> return Nothing
        Just bytes ->
            case decode bytes of
                Left e  -> throwString e
                Right x -> return (Just x)

matchRecursive ::
       ( MonadIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => key
    -> Iterator
    -> ConduitT () (key, value) m ()
matchRecursive base it =
    iterEntry it >>= \case
        Nothing -> return ()
        Just (key_bytes, value_bytes) -> do
            let start_bytes = B.take (B.length base_bytes) key_bytes
            if start_bytes /= base_bytes
                then return ()
                else do
                    key <- either throwString return (decode key_bytes)
                    value <- either throwString return (decode value_bytes)
                    yield (key, value)
                    iterNext it
                    matchRecursive base it
  where
    base_bytes = encode base

-- | Pass a short key to filter all the elements whose key prefix match it. Use
-- a sum type for keys that allows to create a version of the key that
-- serializes to a prefix of a full key.
--
-- > data MyKey = ShortKey String | FullKey String String deriving Show
-- > instance Serialize MyKey where
-- >   put (ShortKey a)  = put a
-- >   put (FullKey a b) = put a >> put b
-- >   get = FullKey <$> get <*> get
-- > instance KeyValue MyKey String
-- > main = do
-- >   db <- open "test-db" defaultOptions {createIfMissing = True}
-- >   insert db (FullKey "hello" "world") "despite all my rage"
-- >   Just record <- runResourceT . runConduit $
-- >     matching db Nothing (ShortKey "hello") .| headC
-- >   print (record :: (MyKey, String))
-- >   -- (Fullkey "hello" "world","despite all my rage")
--
-- In this example the @ShortKey@ is serialized to the prefix of the only
-- element in the database, which is then returned. Since the 'get' function of
-- the 'Serialize' instance for @MyKey@ only understands how to deserialize a
-- @FullKey@, then that is what is returned.
matching ::
       ( MonadResource m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> key
    -> ConduitT () (key, value) m ()
matching db snapshot base = do
    let opts = defaultReadOptions {useSnapshot = snapshot}
    withIterator db opts $ \it -> do
        iterSeek it (encode base)
        matchRecursive base it

-- | Like 'matching', but skip to the second key passed as argument, or after if
-- there is no entry for the second key.
matchingSkip ::
       ( MonadResource m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> key
    -> key
    -> ConduitT () (key, value) m ()
matchingSkip db snapshot base start = do
    let opts = defaultReadOptions {useSnapshot = snapshot}
    withIterator db opts $ \it -> do
        iterSeek it (encode start)
        matchRecursive base it

-- | Insert a record into the database.
insert ::
       (MonadIO m, KeyValue key value, Serialize key, Serialize value)
    => DB
    -> key
    -> value
    -> m ()
insert db key value = R.put db defaultWriteOptions (encode key) (encode value)

-- | Delete a record from the database.
remove :: (MonadIO m, Key key, Serialize key) => DB -> key -> m ()
remove db key = delete db defaultWriteOptions (encode key)

-- | Get the 'BatchOp' to insert a record in the database.
insertOp ::
       (KeyValue key value, Serialize key, Serialize value)
    => key
    -> value
    -> BatchOp
insertOp key value = R.Put (encode key) (encode value)

-- | Get the 'BatchOp' to delete a record from the database.
deleteOp :: (Key key, Serialize key) => key -> BatchOp
deleteOp key = Del (encode key)

-- | Write a batch to the database.
writeBatch :: MonadIO m => DB -> WriteBatch -> m ()
writeBatch db = write db defaultWriteOptions

-- | Like 'matching' but return the first element only.
firstMatching ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> key
    -> m (Maybe (key, value))
firstMatching db snapshot base =
    runResourceT . runConduit $ matching db snapshot base .| headC

-- | Like 'matchingSkip', but return the first element only.
firstMatchingSkip ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> key
    -> key
    -> m (Maybe (key, value))
firstMatchingSkip db snapshot base start =
    runResourceT . runConduit $
    matchingSkip db snapshot base start .| headC

-- | Like 'matching' but return a list.
matchingAsList ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> key
    -> m [(key, value)]
matchingAsList db snapshot base =
    runResourceT . runConduit $
    matching db snapshot base .| sinkList

-- | Like 'matchingSkip', but return a list.
matchingSkipAsList ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> key
    -> key
    -> m [(key, value)]
matchingSkipAsList db snapshot base start =
    runResourceT . runConduit $
    matchingSkip db snapshot base start .| sinkList
