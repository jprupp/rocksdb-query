{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Database.RocksDB.Query where

import qualified Data.ByteString          as B
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC
import           Data.Serialize           as S
import           Database.RocksDB         as R
import           UnliftIO
import           UnliftIO.Resource

class Key key
class KeyValue key value

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

insert ::
       (MonadIO m, KeyValue key value, Serialize key, Serialize value)
    => DB
    -> key
    -> value
    -> m ()
insert db key value = R.put db defaultWriteOptions (encode key) (encode value)

remove :: (MonadIO m, Key key, Serialize key) => DB -> key -> m ()
remove db key = delete db defaultWriteOptions (encode key)

insertOp ::
       (KeyValue key value, Serialize key, Serialize value)
    => key
    -> value
    -> BatchOp
insertOp key value = R.Put (encode key) (encode value)

deleteOp :: (Key key, Serialize key) => key -> BatchOp
deleteOp key = Del (encode key)

writeBatch :: MonadIO m => DB -> WriteBatch -> m ()
writeBatch db = write db defaultWriteOptions

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
    runResourceT . runConduit $ matching db snapshot base .| CC.head

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
    matchingSkip db snapshot base start .| CC.head

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
    matching db snapshot base .| CC.sinkList

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
    matchingSkip db snapshot base start .| CC.sinkList
