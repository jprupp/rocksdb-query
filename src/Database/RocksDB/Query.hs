{-# LANGUAGE LambdaCase #-}
module Database.RocksDB.Query where

import qualified Data.ByteString          as B
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC
import           Data.Serialize           as S
import           Database.RocksDB         as R
import           UnliftIO
import           UnliftIO.Resource

retrieve ::
       (MonadIO m, Serialize key, Serialize value)
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

matching ::
       (MonadResource m, Serialize base, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> base
    -> ConduitT () (key, value) m ()
matching db snapshot base = matchingSkip db snapshot base base

matchingSkip ::
       ( MonadResource m
       , Serialize base
       , Serialize start
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> base
    -> start
    -> ConduitT () (key, value) m ()
matchingSkip db snapshot base start = do
    let opts = defaultReadOptions {useSnapshot = snapshot}
    withIterator db opts $ \it -> do
        iterSeek it (encode start)
        recurse it
  where
    base_bytes = encode base
    recurse it =
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
                        recurse it

insert ::
       (MonadIO m, Serialize key, Serialize value) => DB -> key -> value -> m ()
insert db key value = R.put db defaultWriteOptions (encode key) (encode value)

remove :: (MonadIO m, Serialize key) => DB -> key -> m ()
remove db key = delete db defaultWriteOptions (encode key)

insertOp :: (Serialize key, Serialize value) => key -> value -> BatchOp
insertOp key value = R.Put (encode key) (encode value)

deleteOp :: Serialize key => key -> BatchOp
deleteOp key = Del (encode key)

firstMatching ::
       (MonadIO m, Serialize base, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> base
    -> m (Maybe (key, value))
firstMatching db snapshot base = firstMatchingSkip db snapshot base base

firstMatchingSkip ::
       ( MonadIO m
       , Serialize base
       , Serialize start
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> base
    -> start
    -> m (Maybe (key, value))
firstMatchingSkip db snapshot base start =
    liftIO . runResourceT . runConduit $
    matchingSkip db snapshot base start .| CC.head

matchingAsList ::
       (MonadIO m, Serialize base, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> base
    -> m [(key, value)]
matchingAsList db snapshot base = matchingSkipAsList db snapshot base base

matchingSkipAsList ::
       ( MonadIO m
       , Serialize base
       , Serialize start
       , Serialize key
       , Serialize value
       )
    => DB
    -> Maybe Snapshot
    -> base
    -> start
    -> m [(key, value)]
matchingSkipAsList db snapshot base start =
    liftIO . runResourceT . runConduit $
    matchingSkip db snapshot base start .| CC.sinkList
