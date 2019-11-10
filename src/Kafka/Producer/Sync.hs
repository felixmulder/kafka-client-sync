-- | This module provides a synchronous interface on top of the hw-kafka-client
--
--   It works by using MVars managed in two different queues. Each request is
--   sent as soon as there are no other effectively equal Kafka records
--   in-flight. This is done in order to make sure that there is no ambiguity
--   as to which MVar to resolve.
--
--   Currently, this implements fair sending. For all requests, the oldest
--   pending request should be sent first.
--
module Kafka.Producer.Sync
  ( -- * Sync producer
    SyncKafkaProducer
  , newSyncProducer
  , produceRecord

    -- * Re-exports

    -- ** Record datatypes
  , ProducerRecord(..)
  , TopicName(..)
  , ProducePartition(..)

    -- ** Errors
  , KafkaError(..)

    -- ** Producer configuration
  , ProducerProperties(..)
    -- ** Configuration helpers
  , KP.brokersList            -- | Set brokers for producer
  , KP.logLevel               -- | Set log-level for producer
  , KP.compression            -- | Set compression level for producer
  , KP.topicCompression       -- | Set topic compression for producer
  , KP.sendTimeout            -- | Set send timeout for producer
  , KP.extraProps             -- | Set extra properties for producer
  , KP.suppressDisconnectLogs -- | Suppress disconnect log lines
  , KP.extraTopicProps        -- | Configure extra topic properties
  , KP.debugOptions           -- | Add 'KafkaDebug' options

    -- ** Other datatypes
  , BrokerAddress(..)
  , KafkaCompressionCodec(..)
  , KafkaDebug(..)
  , KafkaLogLevel(..)
  , Timeout(..)
  )
  where

import           Prelude

import           Control.Concurrent.MVar (MVar, newMVar, takeMVar, newEmptyMVar, putMVar)
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.Foldable (find)
import           Data.Functor ((<&>))
import           Data.Maybe (isJust)
import           Data.Sequence (Seq(..), (<|), (|>))
import qualified Kafka.Producer as KP (deliveryCallback, flushProducer, newProducer)
import qualified Kafka.Producer as KP (produceMessage, setCallback)
import           Kafka.Producer.ProducerProperties (ProducerProperties(..))
import qualified Kafka.Producer.ProducerProperties as KP (brokersList, logLevel, compression, topicCompression)
import qualified Kafka.Producer.ProducerProperties as KP (sendTimeout, extraProps, suppressDisconnectLogs)
import qualified Kafka.Producer.ProducerProperties as KP (extraTopicProps, debugOptions)
import           Kafka.Producer.Types (KafkaProducer, ProducerRecord(..))
import           Kafka.Producer.Types (DeliveryReport(..), ProducePartition(..))
import           Kafka.Types (KafkaLogLevel(..), KafkaError(..), TopicName(..), Timeout(..))
import           Kafka.Types (KafkaDebug(..), BrokerAddress(..), KafkaCompressionCodec(..))

produceRecord :: MonadIO m => SyncKafkaProducer -> ProducerRecord -> m (Either KafkaError ())
produceRecord syncProducer record =
  -- Produce our message synchronously, then send pending:
  liftIO $ sendProducerRecord record syncProducer <* sendPending syncProducer


-- | A producer for sending messages to Kafka and waiting for the 'DeliveryReport'
--
data SyncKafkaProducer = SyncKafkaProducer
  { requests :: MVar Requests
  , producer :: KafkaProducer
  }

-- | A variable containing the MVar that needs to be resolved in order for the
--   caller to proceed
--
type ResultVar = MVar (Either KafkaError ())

data Requests = Requests
  { pending :: Seq (ResultVar, ProducerRecord)
    -- ^ This sequence contains records that are effectively equal to something
    --   that is already being sent. In order for us not to have ambiguities on
    --   what 'MVar' to resolve on 'DeliveryReport's - this separation is needed.
    --
    --   When a sent record has it's 'ResultVar' resolved, an effectively equal
    --   record is removed from this @pending@ queue and produced via the
    --   'KafkaProducer'. Once it's produced, it is moved to @sent@.
  , sent :: Seq (ResultVar, ProducerRecord)
    -- ^ This structure keeps track of in-flight messages sent to the Kafka
    --   broker, but not currently acknowledged. Once they are acknowledged
    --   via the callback action - they are removed from this structure and
    --   their 'ResultVar' is resolved.
  }

instance Show Requests where
  show Requests{..} =
    "Requests { pending = " <> show (snd <$> pending) <> ", sent = " <> show (snd <$> sent) <> " }"

-- | Create a new 'SyncKafkaProducer'
--
--   /Note/: since this library wraps the regular hw-kafka-client, please be
--   aware that you should not set the delivery report callback. As it is set
--   internally.
--
newSyncProducer :: MonadIO m => ProducerProperties -> m (Either KafkaError SyncKafkaProducer)
newSyncProducer props = liftIO $ do
  reqs <- newMVar Requests { pending = mempty, sent = mempty }

  let
    -- A handler that removes requests from sent and resolves callbacks by
    -- putting to mvars:
    callbackAction =
      handleDeliveryReport reqs

    -- The regular hw-kafka-client KafkaProducer, with the sync callback handler:
    producer =
      KP.newProducer $ props <> KP.setCallback (KP.deliveryCallback callbackAction)

  producer <&> fmap (SyncKafkaProducer reqs)

-- | Sends one pending producer record
--
--   This will cause the completed send request to send an additional pending
--   record, and so on, eventually emptying the pending queue.
--
sendPending :: SyncKafkaProducer -> IO ()
sendPending SyncKafkaProducer{..} = do
  reqs <- takeMVar requests
  case pending reqs of
    (mvar, rec) :<| rest -> do
      KP.produceMessage producer rec >>= \case
        Just err -> putMVar mvar . Left $ err
        Nothing -> pure ()
      putMVar requests reqs { pending = rest, sent = sent reqs |> (mvar, rec) }
      KP.flushProducer producer
    Empty ->
      putMVar requests reqs

-- | Sends a producer record and waits for its delivery report
sendProducerRecord :: ProducerRecord -> SyncKafkaProducer -> IO (Either KafkaError ())
sendProducerRecord record SyncKafkaProducer{..} =
  takeMVar requests >>= \reqs ->
    if hasEffectivelyEqual record (sent reqs) then do
      var <- newEmptyMVar
      putMVar requests reqs { pending = pending reqs |> (var, record) }
      takeMVar var
    else KP.produceMessage producer record >>= \case
      Just err -> do
        putMVar requests reqs
        pure (Left err)
      Nothing -> do
        var <- newEmptyMVar
        putMVar requests reqs { sent = sent reqs |> (var, record) }
        KP.flushProducer producer
        takeMVar var

hasEffectivelyEqual :: ProducerRecord -> Seq (a, ProducerRecord) -> Bool
hasEffectivelyEqual record
  = isJust
  . find (effectivelyEqual record)
  . fmap snd

handleDeliveryReport :: MVar Requests -> (DeliveryReport -> IO ())
handleDeliveryReport mvarRequests = \case
  DeliverySuccess record _offset -> do
    reqs <- takeMVar mvarRequests
    case getAndRemove record (sent reqs) of
      Just (mvar, rest) -> do
        putMVar mvarRequests reqs { sent = rest }
        putMVar mvar $ Right ()
      Nothing ->
        error
          $ "Illegal state ocurred, record was not in sent: "
          <> show reqs
  DeliveryFailure record err -> do
    reqs <- takeMVar mvarRequests
    case getAndRemove record (sent reqs) of
      Just (mvar, rest) -> do
        putMVar mvarRequests reqs { sent = rest }
        putMVar mvar . Left $ err
      Nothing ->
        error
          $ "Illegal state ocurred, record was not in sent: "
          <> show reqs
  NoMessageError err ->
    error $ "Illegal state ocurred, NoMessageError received: " <> show err

getAndRemove ::
     ProducerRecord
  -> Seq (ResultVar, ProducerRecord)
  -> Maybe (ResultVar, Seq (ResultVar, ProducerRecord))
getAndRemove record xs =
  let
    splitRight acc = \case
      rest :|> current ->
        if snd current `effectivelyEqual` record then
          Just (fst current, rest <> acc)
        else
          splitRight (current <| acc) rest
      Empty -> Nothing
  in
    splitRight Empty xs

effectivelyEqual :: ProducerRecord -> ProducerRecord -> Bool
effectivelyEqual this other =
  prTopic this == prTopic other &&
  prKey this == prKey other &&
  prValue this == prValue other
