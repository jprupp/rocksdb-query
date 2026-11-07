# rocksdb-query

Type-safe helpers to store and query Haskell data types in
[RocksDB](https://rocksdb.org), built on top of
[rocksdb-haskell-jprupp](https://github.com/jprupp/rocksdb-haskell).

Keys and values are serialized with
[cereal](https://hackage.haskell.org/package/cereal). Empty type classes link
each key type to its value type, so you cannot read a value of the wrong
type by mistake. Prefix queries let you fetch every record whose key starts
with a given short key, as a list or as a
[conduit](https://hackage.haskell.org/package/conduit) stream.

## Usage

```haskell
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}
import Data.Default (def)
import Data.Serialize (Serialize (..))
import Data.Word (Word32)
import Database.RocksDB (Config (..), withDB)
import Database.RocksDB.Query
import GHC.Generics (Generic)

-- Orders are keyed by customer, then by order number.
data OrderKey
    = OrderKey Word32 Word32   -- customer, order
    | OrderKeyS Word32         -- customer only: a prefix of OrderKey
    deriving (Show, Generic)

data Order = Order { item :: String, qty :: Int }
    deriving (Show, Generic)

instance Serialize Order

-- The prefix key must serialize to a prefix of the full key, so write the
-- instance by hand instead of deriving it.
instance Serialize OrderKey where
    put (OrderKey c o) = put c >> put o
    put (OrderKeyS c)  = put c
    get = OrderKey <$> get <*> get

instance Key OrderKey
instance KeyValue OrderKey Order

main :: IO ()
main = withDB "/tmp/orders" def{createIfMissing = True} $ \db -> do
    insert db (OrderKey 1 100) (Order "apple" 3)
    writeBatch db
        [ insertOp (OrderKey 1 101) (Order "pear" 1)
        , insertOp (OrderKey 2 200) (Order "plum" 7)
        ]

    retrieve db (OrderKey 1 100) >>= print
    -- Just (Order {item = "apple", qty = 3})

    matchingAsList db (OrderKeyS 1) >>= mapM_ print
    -- (OrderKey 1 100,Order {item = "apple", qty = 3})
    -- (OrderKey 1 101,Order {item = "pear", qty = 1})

    remove db (OrderKey 2 200)
```

## API overview

| Function                                   | Purpose                                        |
|--------------------------------------------|------------------------------------------------|
| `retrieve`                                 | Read a value by key.                           |
| `insert`, `remove`                         | Write or delete a single record.               |
| `insertOp`, `deleteOp`, `writeBatch`       | Build and apply an atomic batch.               |
| `matching`, `matchingSkip`                 | Stream all records under a prefix (conduit).   |
| `matchingAsList`, `matchingSkipAsList`     | Same, collected into a list.                   |
| `firstMatching`, `firstMatchingSkip`       | First record under a prefix.                   |

Each function has a `CF` variant that works on a column family. The `Skip`
variants start at a given key inside the prefix, which is handy for
pagination.

If a stored key or value fails to decode, these functions call `error`: that
means the `Serialize` instances used for writing and reading disagree.

## Building and testing

You need the RocksDB C library (`librocksdb-dev` on Debian/Ubuntu,
`rocksdb-devel` on Fedora, `rocksdb` on Homebrew).

```sh
stack build
stack test
```

## License

MIT. See [LICENSE](LICENSE).
