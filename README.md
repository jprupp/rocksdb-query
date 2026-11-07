# rocksdb-query

[![Hackage](https://img.shields.io/hackage/v/rocksdb-query.svg)](https://hackage.haskell.org/package/rocksdb-query)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/jprupp/rocksdb-query/blob/master/LICENSE)

rocksdb-query stores and queries Haskell data types in
[RocksDB](https://rocksdb.org). It sits on top of
[rocksdb-haskell-jprupp](https://github.com/jprupp/rocksdb-haskell) and adds
three things:

- Keys and values are any types with a
  [cereal](https://hackage.haskell.org/package/cereal) `Serialize` instance.
- Empty type classes tie each key type to its value type, so the compiler stops
  you from reading a value of the wrong type.
- Prefix queries fetch every record whose key starts with a short key, as a list
  or as a [conduit](https://hackage.haskell.org/package/conduit) stream.

- [API documentation](https://hackage.haskell.org/package/rocksdb-query)
- [Changelog](https://github.com/jprupp/rocksdb-query/blob/master/CHANGELOG.md)
- [Report a bug](https://github.com/jprupp/rocksdb-query/issues)

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Usage guide](#usage-guide)
- [API overview](#api-overview)
- [Designing keys](#designing-keys)
- [Development](#development)
- [Related projects](#related-projects)
- [License](#license)

## Installation

### System library

You need the RocksDB C library, as for
[rocksdb-haskell-jprupp](https://github.com/jprupp/rocksdb-haskell#system-library):

```sh
sudo apt-get install librocksdb-dev   # Debian and Ubuntu
sudo dnf install rocksdb-devel        # Fedora
brew install rocksdb                  # macOS with Homebrew
```

### With Cabal

```cabal
build-depends:
    base
  , cereal
  , data-default
  , rocksdb-haskell-jprupp
  , rocksdb-query
```

You open and close the database with `rocksdb-haskell-jprupp`, and write the
`Serialize` instances with `cereal`.

### With Stack

The package is in Stackage nightly snapshots. Otherwise add it to `stack.yaml`:

```yaml
extra-deps:
  - rocksdb-query-0.6.0
  - rocksdb-haskell-jprupp-2.6.0
```

## Quick start

Orders keyed by customer, then by order number:

```haskell
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}

import Data.Default (def)
import Data.Serialize (Serialize (..))
import Data.Word (Word32)
import Database.RocksDB (Config (..), withDB)
import Database.RocksDB.Query
import GHC.Generics (Generic)

data OrderKey
  = OrderKey Word32 Word32 -- customer, order
  | OrderKeyS Word32 -- customer only: a prefix of OrderKey
  deriving (Show, Generic)

data Order = Order {item :: String, qty :: Int}
  deriving (Show, Generic)

instance Serialize Order

-- The short key must serialize to a prefix of the full key, so write this
-- instance by hand instead of deriving it.
instance Serialize OrderKey where
  put (OrderKey c o) = put c >> put o
  put (OrderKeyS c) = put c
  get = OrderKey <$> get <*> get

instance Key OrderKey

instance KeyValue OrderKey Order

main :: IO ()
main = withDB "/tmp/orders" def {createIfMissing = True} $ \db -> do
  insert db (OrderKey 1 100) (Order "apple" 3)
  writeBatch
    db
    [ insertOp (OrderKey 1 101) (Order "pear" 1),
      insertOp (OrderKey 2 200) (Order "plum" 7)
    ]

  retrieve db (OrderKey 1 100) >>= print
  -- Just (Order {item = "apple", qty = 3})

  matchingAsList db (OrderKeyS 1) >>= mapM_ print
  -- (OrderKey 1 100,Order {item = "apple", qty = 3})
  -- (OrderKey 1 101,Order {item = "pear", qty = 1})

  remove db (OrderKey 2 200)
```

## Concepts

### Typed keys

`Key` and `KeyValue` have no methods. They are declarations:

```haskell
instance Key OrderKey             -- OrderKey can be deleted
instance KeyValue OrderKey Order  -- values under OrderKey are Orders
```

`retrieve`, `insert` and the queries need `KeyValue`; `remove` and `deleteOp`
need only `Key`. Reading a key as a value type you did not declare for it is
a type error, rather than a decoding failure at run time.

### Prefix queries

RocksDB keeps keys sorted by their bytes, so all keys that start with the same
bytes sit next to each other. A query with a short key seeks to the first key
at or after its encoding and reads until a key no longer starts with it.

cereal encodes unsigned integers (`Word32`, `Word64`) in big-endian order, so
those key parts sort in numeric order. Signed integers do not: negative numbers
sort after positive ones.

### Decoding errors

If a key or value in the database cannot be decoded, these functions call
`error`. That means the instance used to read disagrees with the one used to
write, or two key types share the same prefix.

## Usage guide

### Reading and writing

```haskell
insert db key value
mvalue <- retrieve db key
remove db key
```

### Batches

All the operations in a batch are applied atomically:

```haskell
writeBatch db [insertOp k1 v1, insertOp k2 v2, deleteOp k3]
```

### Prefix queries

| Function            | Returns                                      |
| ------------------- | -------------------------------------------- |
| `matchingAsList`    | every match, as a list                       |
| `firstMatching`     | the first match                              |
| `matching`          | every match, as a conduit, on your iterator  |

The conduit version reads lazily, so it suits prefixes with many matches:

```haskell
import Conduit
import Database.RocksDB (withIter)

total <- withIter db $ \it ->
  runConduit $ matching it (OrderKeyS 1) .| mapC (qty . snd) .| sumC
```

### Pagination

The `Skip` variants start inside the prefix at a given key, or the next one
after it:

```haskell
page2 <- take 20 <$> matchingSkipAsList db (OrderKeyS 1) (OrderKey 1 lastSeen)
```

### Column families

Each function has a `CF` variant that takes a `ColumnFamily`:

```haskell
withDBCF dir cfg [("orders", cfg)] $ \db -> do
  let [orders] = columnFamilies db
  insertCF db orders (OrderKey 1 100) (Order "apple" 3)
  matchingAsListCF db orders (OrderKeyS 1)
```

## API overview

| Default column family                  | Column family variant                      |
| -------------------------------------- | ------------------------------------------ |
| `retrieve`                             | `retrieveCF`                               |
| `insert`, `remove`                     | `insertCF`, `removeCF`                     |
| `insertOp`, `deleteOp`                 | `insertOpCF`, `deleteOpCF`                 |
| `matchingAsList`, `matchingSkipAsList` | `matchingAsListCF`, `matchingSkipAsListCF` |
| `firstMatching`, `firstMatchingSkip`   | `firstMatchingCF`, `firstMatchingSkipCF`   |
| `matching`, `matchingSkip`             | use an iterator from `withIterCF`          |

`writeBatch` takes operations for any column family. Everything is in
`Database.RocksDB.Query`.

## Designing keys

- Start every key type with a distinct tag byte when several types share a
  column family, so that their prefixes never overlap:

  ```haskell
  put (OrderKey c o) = putWord8 0x01 >> put c >> put o
  put (OrderKeyS c) = putWord8 0x01 >> put c
  ```

- Give the short key the same leading fields, in the same order, as the full
  key. `get` only needs to decode the full key.
- Use fixed-width, big-endian numbers (`Word32`, `Word64`) for parts you sort
  or range over. Variable-length encodings do not sort numerically.
- To read newest first, store an inverted value, such as `maxBound - time`.

## Development

```sh
git clone https://github.com/jprupp/rocksdb-query.git
cd rocksdb-query

stack build                        # build the library
stack test                         # run the tests
stack haddock --no-haddock-deps    # build the API documentation
```

With `stack --nix`, the `nix` section of `stack.yaml` provides RocksDB.

`package.yaml` is the source of the package description. Change it rather than
`rocksdb-query.cabal`, which [hpack](https://github.com/sol/hpack) generates.

The tests are in `test/Spec.hs` and use [hspec](https://hspec.github.io/).

### Contributing

Pull requests are welcome.

- Keep the style of the code around your change.
- Add tests for the behaviour you change.
- Describe the change in `CHANGELOG.md`, which follows
  [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Related projects

- [rocksdb-haskell-jprupp](https://github.com/jprupp/rocksdb-haskell) is the
  RocksDB binding underneath.
- [haskoin-node](https://github.com/jprupp/haskoin-node) and
  [haskoin-store](https://github.com/jprupp/haskoin-store) store their headers
  and indices with this library.

## License

rocksdb-query is released under the
[MIT License](https://github.com/jprupp/rocksdb-query/blob/master/LICENSE).
