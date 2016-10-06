(ns onyx.messaging.lambda
  (:require [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as messenger]
            [onyx.types :as types]))


(defrecord LambdaMessengerPeerGroup []
  messenger/MessengerGroup
  (peer-site [messenger peer-id]
    {})

  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    component))

(defn lambda-messenger-group [peer-config]
  (map->LambdaMessengerPeerGroup {}))

(defmethod messenger/build-messenger-group :lambda
  [peer-config]
  (lambda-messenger-group peer-config))

(defmethod messenger/assign-task-resources :lambda
  [replica peer-id task-id peer-site peer-sites]
  {})

(defmethod messenger/get-peer-site :lambda [peer-config]
  {})


(defrecord LambdaMessenger [lambda-request]
  messenger/Messenger
  (start [messenger])
  (stop [messenger])
  (add-subscription [messenger sub])
  (add-ack-subscription [messenger sub])
  (remove-subscription [messenger sub])
  (remove-ack-subscription [messenger sub])
  (add-publication [messenger pub])
  (remove-publication [messenger pub])
  (publications [messenger])
  (subscriptions [messenger])
  (ack-subscriptions [messenger])

  (register-ticket [messenger sub-info])
  (poll [messenger])
  (poll-recover [messenger])

  (offer-segments [messenger messages task-slots])
  (emit-barrier [messenger publication])
  (emit-barrier [messenger publication barrier-opts])
  (emit-barrier-ack [messenger publication])
  (unblock-subscriptions! [messenger])
  (replica-version [messenger])
  (set-replica-version [messenger replica-version])
  (epoch [messenger])
  (next-epoch [messenger])
  (set-epoch [messenger epoch])
  (all-acks-seen? [messenger])
  (all-barriers-seen? [messenger])

  ;; Try to remove multi phase receive/flush. 
  ;; Required for immutable testing version
  (poll-acks [messenger])
  (flush-acks [messenger]))


(defmethod messenger/build-messenger :lambda [peer-config messenger-group id]
  (map->LambdaMessenger {:peer-config peer-config
                         :messenger-group messenger-group
                         :id id}))
