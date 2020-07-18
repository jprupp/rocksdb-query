{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-|
Module      : Database.RocksDB.Query
Copyright   : No rights reserved
License     : UNLICENSE
Maintainer  : xenog@protonmail.com
Stability   : experimental
Portability : POSIX

Query functions to make interaction with RocksDB stores easier and safer.
-}
module Database.RocksDB.Query where

import           Conduit
import           Control.Monad
import qualified Data.ByteString  as B
import           Data.Serialize   as S
import           Database.RocksDB as R
import           UnliftIO

-- | Class for types that are database keys.
class Key key

-- | Class for types that are corresponding database key and value.
class KeyValue key value

-- | Read a value from the database, or 'Nothing' if not found.
retrieve ::
       (MonadIO m, KeyValue key value, Serialize key, Serialize value)
    => DB
    -> ReadOpts
    -> key
    -> m (Maybe value)
retrieve db opts key =
    R.getReadOpts db opts (encode key) >>= \case
        Nothing -> return Nothing
        Just bytes ->
            case decode bytes of
                Left e  -> throwString e
                Right x -> return (Just x)

matchRecursiveList ::
     (MonadIO m, KeyValue key value, Serialize key, Serialize value)
  => key
  -> Iterator
  -> m [(key, value)]
matchRecursiveList base it = go
  where
    go = iterEntry it >>= \case
        Nothing -> return []
        Just (key_bytes, value_bytes) ->
          if base_bytes `B.isPrefixOf` key_bytes
              then do
                  key <- either throwString return (decode key_bytes)
                  value <- either throwString return (decode value_bytes)
                  iterNext it
                  ((key, value) :) <$> go
              else return []
    base_bytes = encode base

-- | Internal function for recursively matching a key.
matchRecursive ::
       ( MonadIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => key
    -> Iterator
    -> ConduitT i (key, value) m ()
matchRecursive base it = go
  where
    go =
      iterEntry it >>= \case
        Nothing -> return ()
        Just (key_bytes, value_bytes) ->
            when (base_bytes `B.isPrefixOf` key_bytes) $ do
                key <- either throwString return (decode key_bytes)
                value <- either throwString return (decode value_bytes)
                yield (key, value)
                iterNext it
                go
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
-- >     matching db def (ShortKey "hello") .| headC
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
    -> ReadOpts
    -> key
    -> ConduitT i (key, value) m ()
matching db opts base = do
    it <- createIter db opts
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
    -> ReadOpts
    -> key
    -> key
    -> ConduitT i (key, value) m ()
matchingSkip db opts base start = do
    it <- createIter db opts
    iterSeek it (encode start)
    matchRecursive base it

-- | Insert a record into the database.
insert ::
       (MonadIO m, KeyValue key value, Serialize key, Serialize value)
    => DB
    -> key
    -> value
    -> m ()
insert db key value = R.put db (encode key) (encode value)

-- | Delete a record from the database.
remove :: (MonadIO m, Key key, Serialize key) => DB -> key -> m ()
remove db key = delete db (encode key)

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
writeBatch :: MonadIO m => DB -> [BatchOp] -> m ()
writeBatch = write

-- | Like 'matching' but return the first element only.
firstMatching ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> ReadOpts
    -> key
    -> m (Maybe (key, value))
firstMatching db opts base =
    runResourceT . runConduit $ matching db opts base .| headC

-- | Like 'matchingSkip', but return the first element only.
firstMatchingSkip ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> ReadOpts
    -> key
    -> key
    -> m (Maybe (key, value))
firstMatchingSkip db opts base start =
    runResourceT . runConduit $
    matchingSkip db opts base start .| headC

-- | Like 'matching' but return a list.
matchingAsList ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> ReadOpts
    -> key
    -> m [(key, value)]
matchingAsList db opts base = do
    it <- createIter db opts
    iterSeek it (encode base)
    matchRecursiveList base it

-- | Like 'matchingSkip', but return a list.
matchingSkipAsList ::
       ( MonadUnliftIO m
       , KeyValue key value
       , Serialize key
       , Serialize value
       )
    => DB
    -> ReadOpts
    -> key
    -> key
    -> m [(key, value)]
matchingSkipAsList db opts base start = do
    it <- createIter db opts
    iterSeek it (encode start)
    matchRecursiveList base it
