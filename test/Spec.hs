import           Control.Monad
import           Data.Serialize         as S
import           Data.Word
import           Database.RocksDB       as R
import           Database.RocksDB.Query
import           Test.Hspec
import           UnliftIO

newtype KeyOne = KeyOne Word32 deriving (Show, Eq)
data KeyTwo = KeyTwo Word32 Word32 deriving (Show, Eq)
newtype KeyTwoBase = KeyTwoBase Word32 deriving (Show, Eq)

newtype ValueOne = ValueOne String deriving (Show, Eq)
newtype ValueTwo = ValueTwo String deriving (Show, Eq)

instance Serialize KeyOne where
    put (KeyOne x) = do
        putWord8 0x01
        S.put x
    get = do
        getWord8 >>= guard . (== 0x01)
        KeyOne <$> S.get

instance Serialize KeyTwo where
    put (KeyTwo x y) = do
        S.put (KeyTwoBase x)
        S.put y
    get = do
        KeyTwoBase x <- S.get
        KeyTwo x <$> S.get

instance Serialize KeyTwoBase where
    put (KeyTwoBase x) = do
        putWord8 0x02
        S.put x
    get = do
        getWord8 >>= guard . (== 0x02)
        KeyTwoBase <$> S.get

instance Serialize ValueOne where
    put (ValueOne s) = S.put s
    get = ValueOne <$> S.get

instance Serialize ValueTwo where
    put (ValueTwo s) = S.put s
    get = ValueTwo <$> S.get

main :: IO ()
main =
    setup $ \db ->
        describe "database" $ do
            it "reads a record" $ do
                r <- retrieve db Nothing (KeyTwo 1 2)
                r `shouldBe` Just "Hello First World Again!"
            it "reads two records at the end" $ do
                let ls =
                        [ (KeyTwo 2 1, "Hello Second World!")
                        , (KeyTwo 2 2, "Hello Second World Again!")
                        ]
                rs <- matchingAsList db Nothing (KeyTwoBase 2)
                rs `shouldBe` ls
            it "reads two records in the middle" $ do
                let ls =
                        [ (KeyTwo 1 1, "Hello First World!")
                        , (KeyTwo 1 2, "Hello First World Again!")
                        ]
                rs <- matchingAsList db Nothing (KeyTwoBase 1)
                rs `shouldBe` ls
            it "query and skip" $ do
                let ex = (KeyTwo 2 2, "Hello Second World Again!")
                rs <- matchingSkipAsList db Nothing (KeyTwoBase 2) (KeyTwo 2 2)
                rs `shouldBe` [ex]
  where
    setup f =
        withSystemTempDirectory "rocksdb-query-test-" $ \d -> do
            db <- open d defaultOptions {createIfMissing = True}
            insertTestRecords db
            hspec $ f db

insertTestRecords :: MonadIO m => DB -> m ()
insertTestRecords db = do
    insert db (KeyOne 1) "Hello World!"
    insert db (KeyOne 2) "Hello World Again!"
    insert db (KeyTwo 1 1) "Hello First World!"
    insert db (KeyTwo 1 2) "Hello First World Again!"
    insert db (KeyTwo 2 1) "Hello Second World!"
    insert db (KeyTwo 2 2) "Hello Second World Again!"
