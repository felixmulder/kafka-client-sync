module Main (main) where

import           Prelude hiding (sequence)

import           Control.Monad (when)
import           Control.Monad.Parallel (sequence)
import           Data.Foldable (for_)
import           Data.Either (isLeft)
import           Data.Text (pack)
import           Data.Text.Encoding (encodeUtf8)
import           Kafka.Producer.Sync
import           Kafka.Producer

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

main :: IO ()
main =
  newSyncProducer producerProps >>= \case
    Left _ -> error "Couldn't create sync producer"
    Right producer -> do
      putStrLn "Running Kafka sync tests"

      let action = produceRecord producer . message
      res <- sequence $
        fmap action [1..100] ++
        fmap action (replicate 1 100)

      for_ res $
        either (putStrLn . (<>) "got error: " . show) pure

      when
        (any isLeft res)
        (error "Couldn't publish messages in parallel")

      putStrLn "Successfully published messages"
