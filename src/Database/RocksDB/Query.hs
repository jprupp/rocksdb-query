{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- |
-- Module      : Database.RocksDB.Query
-- Copyright   : No rights reserved
-- License     : UNLICENSE
-- Maintainer  : xenog@protonmail.com
-- Stability   : experimental
-- Portability : POSIX
--
-- Query functions to make interaction with RocksDB stores easier and safer.
module Database.RocksDB.Query where

import Conduit
import Control.Monad
import Data.ByteString qualified as B
import Data.Serialize as S
import Database.RocksDB as R

-- | Class for types that are database keys.
class Key key

-- | Class for types that are corresponding database key and value.
class KeyValue key value

-- | Read a value from the database, or 'Nothing' if not found.
retrieve ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  key ->
  IO (Maybe value)
retrieve db = retrieveCommon db Nothing

retrieveCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  ColumnFamily ->
  key ->
  IO (Maybe value)
retrieveCF db cf = retrieveCommon db (Just cf)

-- | Read a value from the database, or 'Nothing' if not found.
retrieveCommon ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  Maybe ColumnFamily ->
  key ->
  IO (Maybe value)
retrieveCommon db mcf key =
  f >>= \case
    Nothing -> return Nothing
    Just bytes ->
      case decode bytes of
        Left e -> error e
        Right x -> return (Just x)
  where
    f = case mcf of
      Just cf -> R.getCF db cf (encode key)
      Nothing -> R.get db (encode key)

matchRecursiveList ::
  (KeyValue key value, Serialize key, Serialize value) =>
  key ->
  Iterator ->
  IO [(key, value)]
matchRecursiveList base it = go
  where
    go =
      iterEntry it >>= \case
        Nothing -> return []
        Just (key_bytes, value_bytes) ->
          if base_bytes `B.isPrefixOf` key_bytes
            then do
              key <- either error return (decode key_bytes)
              value <- either error return (decode value_bytes)
              iterNext it
              ((key, value) :) <$> go
            else return []
    base_bytes = encode base

-- | Internal function for recursively matching a key.
matchRecursive ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  key ->
  Iterator ->
  ConduitT i (key, value) IO ()
matchRecursive base it = go
  where
    go =
      liftIO (iterEntry it) >>= \case
        Nothing -> return ()
        Just (key_bytes, value_bytes) ->
          when (base_bytes `B.isPrefixOf` key_bytes) $ do
            key <- either error return (decode key_bytes)
            value <- either error return (decode value_bytes)
            yield (key, value)
            liftIO (iterNext it)
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
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  Iterator ->
  key ->
  ConduitT i (key, value) IO ()
matching it base = do
  liftIO (iterSeek it (encode base))
  matchRecursive base it

-- | Like 'matching', but skip to the second key passed as argument, or after if
-- there is no entry for the second key.
matchingSkip ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  Iterator ->
  key ->
  key ->
  ConduitT i (key, value) IO ()
matchingSkip it base start = do
  liftIO (iterSeek it (encode start))
  matchRecursive base it

-- | Insert a record into the database.
insert ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  key ->
  value ->
  IO ()
insert db key value = R.put db (encode key) (encode value)

-- | Insert a record into the database.
insertCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  ColumnFamily ->
  key ->
  value ->
  IO ()
insertCF db cf key value = R.putCF db cf (encode key) (encode value)

-- | Delete a record from the database.
remove :: (Key key, Serialize key) => DB -> key -> IO ()
remove db key = delete db (encode key)

-- | Delete a record from the database.
removeCF ::
  (Key key, Serialize key) =>
  DB -> ColumnFamily -> key -> IO ()
removeCF db cf key = deleteCF db cf (encode key)

-- | Get the 'BatchOp' to insert a record in the database.
insertOp ::
  (KeyValue key value, Serialize key, Serialize value) =>
  key ->
  value ->
  BatchOp
insertOp key value = R.Put (encode key) (encode value)

-- | Get the 'BatchOp' to insert a record in the database.
insertOpCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  ColumnFamily ->
  key ->
  value ->
  BatchOp
insertOpCF cf key value = R.PutCF cf (encode key) (encode value)

-- | Get the 'BatchOp' to delete a record from the database.
deleteOp :: (Key key, Serialize key) => key -> BatchOp
deleteOp key = Del (encode key)

-- | Get the 'BatchOp' to delete a record from the database.
deleteOpCF :: (Key key, Serialize key) => ColumnFamily -> key -> BatchOp
deleteOpCF cf key = DelCF cf (encode key)

-- | Write a batch to the database.
writeBatch :: DB -> [BatchOp] -> IO ()
writeBatch = write

-- | Like 'matching' but return the first element only.
firstMatching ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  key ->
  IO (Maybe (key, value))
firstMatching db base =
  withIter db $ \it -> runConduit $ matching it base .| headC

-- | Like 'matching' but return the first element only.
firstMatchingCF ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  ColumnFamily ->
  key ->
  IO (Maybe (key, value))
firstMatchingCF db cf base =
  withIterCF db cf $ \it -> runConduit $ matching it base .| headC

-- | Like 'matchingSkip', but return the first element only.
firstMatchingSkip ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  key ->
  key ->
  IO (Maybe (key, value))
firstMatchingSkip db base start =
  withIter db $ \it ->
    runConduit $
      matchingSkip it base start .| headC

-- | Like 'matchingSkip', but return the first element only.
firstMatchingSkipCF ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  ColumnFamily ->
  key ->
  key ->
  IO (Maybe (key, value))
firstMatchingSkipCF db cf base start =
  withIterCF db cf $ \it ->
    runConduit $
      matchingSkip it base start .| headC

-- | Like 'matching' but return a list.
matchingAsList ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  key ->
  IO [(key, value)]
matchingAsList db base =
  withIter db $ \it -> do
    iterSeek it (encode base)
    matchRecursiveList base it

-- | Like 'matching' but return a list.
matchingAsListCF ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  ColumnFamily ->
  key ->
  IO [(key, value)]
matchingAsListCF db cf base =
  withIterCF db cf $ \it -> do
    iterSeek it (encode base)
    matchRecursiveList base it

-- | Like 'matchingSkip', but return a list.
matchingSkipAsList ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  key ->
  key ->
  IO [(key, value)]
matchingSkipAsList db base start =
  withIter db $ \it -> do
    iterSeek it (encode start)
    matchRecursiveList base it

-- | Like 'matchingSkip', but return a list.
matchingSkipAsListCF ::
  ( KeyValue key value,
    Serialize key,
    Serialize value
  ) =>
  DB ->
  ColumnFamily ->
  key ->
  key ->
  IO [(key, value)]
matchingSkipAsListCF db cf base start =
  withIterCF db cf $ \it -> do
    iterSeek it (encode start)
    matchRecursiveList base it
