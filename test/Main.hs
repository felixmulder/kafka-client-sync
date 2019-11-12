module Main (main) where

import           Prelude hiding (sequence)

import           Control.Exception (throw, bracket)
import           Control.Monad (when)
import           Control.Monad.Parallel (sequence)
import           Data.Foldable (for_)
import           Data.Either (isLeft)
import           Data.Text (pack)
import           Data.Text.Encoding (encodeUtf8)
import           Kafka.Producer.Sync

producerProps :: ProducerProperties
producerProps = brokersList [BrokerAddress "localhost:9092"]
  <> sendTimeout (Timeout 10000)
  <> logLevel KafkaLogDebug

message :: Int -> ProducerRecord
message suffix = ProducerRecord
  { prTopic = TopicName "topic"
  , prPartition = UnassignedPartition
  , prKey = Nothing
  , prValue = Just . encodeUtf8 . pack . show $ suffix
  }

withProducer :: (SyncKafkaProducer -> IO ()) -> IO ()
withProducer action =
  let
    newProducerOrThrow = newSyncProducer producerProps >>= \case
      Left err ->
        throw . userError $ "Couldn't start producer: " <> show err
      Right producer ->
        pure producer
  in
    bracket newProducerOrThrow closeSyncProducer action

main :: IO ()
main =
  withProducer $ \producer -> do
    putStrLn "Running Kafka sync tests"

    let action = produceRecord producer . message
    res <- sequence $
      fmap action [1..100] ++
      fmap action (replicate 1 100)

    for_ res $
      either (putStrLn . (<>) "got error: " . show) pure

    when
      (any isLeft res)
      (throw . userError $ "Couldn't publish messages in parallel")

    putStrLn "Successfully published messages"
