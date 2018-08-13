{-# LANGUAGE LambdaCase #-}
module Database.RocksDB.Query where

import qualified Data.ByteString          as B
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC
import           Data.Serialize           as S
import           Database.RocksDB         as R
import           UnliftIO
import           UnliftIO.Resource

query ::
       (MonadIO m, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> key
    -> m (Maybe value)
query db snapshot key = do
    let opts = defaultReadOptions {useSnapshot = snapshot}
    R.get db opts (encode key) >>= \case
        Nothing -> return Nothing
        Just bytes ->
            case decode bytes of
                Left e  -> throwString e
                Right x -> return (Just x)

queryConduit ::
       (MonadResource m, Serialize base, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> base
    -> ConduitT () (key, value) m ()
queryConduit db snapshot base = queryConduitSkip db snapshot base base

queryConduitSkip ::
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
queryConduitSkip db snapshot base start = do
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

queryFirst ::
       (MonadIO m, Serialize base, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> base
    -> m (Maybe (key, value))
queryFirst db snapshot base = queryFirstSkip db snapshot base base

queryFirstSkip ::
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
queryFirstSkip db snapshot base start =
    liftIO . runResourceT . runConduit $
    queryConduitSkip db snapshot base start .| CC.head

queryList ::
       (MonadIO m, Serialize base, Serialize key, Serialize value)
    => DB
    -> Maybe Snapshot
    -> base
    -> m [(key, value)]
queryList db snapshot base = queryListSkip db snapshot base base

queryListSkip ::
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
queryListSkip db snapshot base start =
    liftIO . runResourceT . runConduit $
    queryConduitSkip db snapshot base start .| CC.sinkList
