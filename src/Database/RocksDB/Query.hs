{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- |
-- Module      : Database.RocksDB.Query
-- Copyright   : Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Query functions to make interaction with RocksDB stores easier and safer.
--
-- Keys and values are Haskell types with 'Serialize' instances from the
-- @cereal@ package. They are encoded with 'encode' before being written and
-- decoded with 'decode' after being read. The empty classes 'Key' and
-- 'KeyValue' tie key types to value types, so that the type checker prevents
-- reading a value of the wrong type for a key:
--
-- @
-- data UserKey = UserKey Word32 deriving (Generic)
-- instance 'Serialize' UserKey
-- instance 'Key' UserKey
-- instance 'KeyValue' UserKey User
--
-- getUser :: DB -> Word32 -> IO (Maybe User)
-- getUser db = 'retrieve' db . UserKey
-- @
--
-- = Prefix queries
--
-- Because RocksDB keeps keys sorted by their bytes, all keys that start with
-- the same bytes are stored next to each other. The 'matching' family of
-- functions takes a short key whose encoding is a prefix of the encoding of
-- full keys, and returns every entry whose key starts with that prefix. See
-- 'matching' for an example.
--
-- = Decoding errors
--
-- If a stored key or value cannot be decoded, these functions call 'error'.
-- That indicates a bug, such as a mismatch between the 'Serialize' instance
-- used to write and to read, or two key types sharing a prefix.
--
-- = Column families
--
-- Functions ending in @CF@ act on a given column family. The others act on the
-- default column family.
module Database.RocksDB.Query where

import Conduit
import Control.Monad
import Data.ByteString qualified as B
import Data.Serialize as S
import Database.RocksDB as R

-- | Class for types that are database keys. It has no methods; an instance
-- marks a type as allowed to be used as a key in 'remove', 'deleteOp' and
-- related functions.
class Key key

-- | Class for types that are corresponding database key and value. It has no
-- methods; an instance states that values of type @value@ are stored under keys
-- of type @key@. Functions that read or write values require it.
class KeyValue key value

-- | Read a value from the default column family, or 'Nothing' if not found.
-- Calls 'error' if the stored value cannot be decoded.
retrieve ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  key ->
  IO (Maybe value)
retrieve db = retrieveCommon db Nothing

-- | Like 'retrieve', but read from the given column family.
retrieveCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  ColumnFamily ->
  key ->
  IO (Maybe value)
retrieveCF db cf = retrieveCommon db (Just cf)

-- | Read a value from the default column family, or from a column family if
-- one is given. Returns 'Nothing' if not found.
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

-- | Internal function to collect, from the current position of the iterator,
-- every entry whose key starts with the encoding of the given key. Stops at the
-- first entry that does not match.
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

-- | Internal function for recursively matching a key. Like
-- 'matchRecursiveList', but streams the entries through a conduit.
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
-- > main =
-- >   withDB "test-db" def {createIfMissing = True} $ \db -> do
-- >     insert db (FullKey "hello" "world") "despite all my rage"
-- >     Just record <- withIter db $ \it ->
-- >       runConduit $ matching it (ShortKey "hello") .| headC
-- >     print (record :: (MyKey, String))
-- >     -- (FullKey "hello" "world","despite all my rage")
--
-- In this example the @ShortKey@ is serialized to the prefix of the only
-- element in the database, which is then returned. Since the 'get' function of
-- the 'Serialize' instance for @MyKey@ only understands how to deserialize a
-- @FullKey@, then that is what is returned.
--
-- The iterator is moved to the first key at or after the prefix, and the
-- conduit yields entries until it finds a key that does not start with the
-- prefix. The iterator must stay open while the conduit runs. See
-- 'matchingAsList' and 'firstMatching' for versions that manage the iterator
-- themselves.
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
--
-- This is useful for pagination: pass the last key seen in the previous page
-- (or the key just after it) as the second argument. The second key should
-- start with the same prefix as the first; if it sorts before the prefix, no
-- entries are returned.
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

-- | Insert a record into the default column family, replacing any previous
-- value for the key.
insert ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  key ->
  value ->
  IO ()
insert db key value = R.put db (encode key) (encode value)

-- | Like 'insert', but write to the given column family.
insertCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  ColumnFamily ->
  key ->
  value ->
  IO ()
insertCF db cf key value = R.putCF db cf (encode key) (encode value)

-- | Delete a record from the default column family. Deleting a key that does
-- not exist is not an error.
remove :: (Key key, Serialize key) => DB -> key -> IO ()
remove db key = delete db (encode key)

-- | Like 'remove', but delete from the given column family.
removeCF ::
  (Key key, Serialize key) =>
  DB -> ColumnFamily -> key -> IO ()
removeCF db cf key = deleteCF db cf (encode key)

-- | Get the 'BatchOp' to insert a record in the default column family. Use with
-- 'writeBatch' to apply several changes atomically.
insertOp ::
  (KeyValue key value, Serialize key, Serialize value) =>
  key ->
  value ->
  BatchOp
insertOp key value = R.Put (encode key) (encode value)

-- | Like 'insertOp', but for the given column family.
insertOpCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  ColumnFamily ->
  key ->
  value ->
  BatchOp
insertOpCF cf key value = R.PutCF cf (encode key) (encode value)

-- | Get the 'BatchOp' to delete a record from the default column family. Use
-- with 'writeBatch' to apply several changes atomically.
deleteOp :: (Key key, Serialize key) => key -> BatchOp
deleteOp key = Del (encode key)

-- | Like 'deleteOp', but for the given column family.
deleteOpCF :: (Key key, Serialize key) => ColumnFamily -> key -> BatchOp
deleteOpCF cf key = DelCF cf (encode key)

-- | Write a batch to the database atomically. Synonym for 'write'.
--
-- @
-- 'writeBatch' db ['insertOp' k1 v1, 'deleteOp' k2]
-- @
writeBatch :: DB -> [BatchOp] -> IO ()
writeBatch = write

-- | Like 'matching' but return the first element only, or 'Nothing' if no key
-- starts with the prefix. Opens and closes its own iterator.
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

-- | Like 'firstMatching', but read from the given column family.
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

-- | Like 'matchingSkip', but return the first element only. Opens and closes
-- its own iterator.
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

-- | Like 'firstMatchingSkip', but read from the given column family.
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

-- | Like 'matching' but return a list. Opens and closes its own iterator. All
-- matching entries are loaded into memory, so use 'matching' for prefixes that
-- may match many entries.
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

-- | Like 'matchingAsList', but read from the given column family.
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

-- | Like 'matchingSkip', but return a list. Opens and closes its own iterator.
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

-- | Like 'matchingSkipAsList', but read from the given column family.
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
