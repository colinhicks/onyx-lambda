(ns onyx.lambda.system
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as messenger]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.extensions :as extensions]
            [onyx.monitoring.no-op-monitoring]
            [onyx.lambda.task-lifecycle :as lambda.task-lifecycle]
            [onyx.messaging.lambda]))


(defrecord OnyxLambdaSystem []
  component/Lifecycle
  (start [component]
    (component/start-system component))
  (stop [component]
    (component/stop-system component)))

(defrecord CompletionHandler [lambda-request]
  component/Lifecycle
  (start [component]
    (let [outbox-ch (async/promise-chan)
          outbox-thread
          (async/thread
            (loop []
              (when-let [msg (async/<!! outbox-ch)]
                (println "outbox: " msg))))]
      (assoc component
             :outbox-ch outbox-ch
             :outbox-thread outbox-thread)))

  (stop [{:keys [outbox-ch] :as component}]
    (async/close! outbox-ch)
    component))

(defn completion-handler [lambda-request]
  (map->CompletionHandler {:lambda-request lambda-request}))

(defrecord LambdaLog [peer-config]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    component))

(defn lambda-log [peer-config]
  (map->LambdaLog {:peer-config peer-config}))

(defrecord TaskInformation
    [id job-id task-id workflow catalog task flow-conditions
     windows triggers lifecycles metadata]
  component/Lifecycle
  (start [component] component)
  (stop [component] component))

(defn create-task-information [id job task task-map]
  (map->TaskInformation
   {:id id
    :job-id (get-in job [:metadata :job-id])
    :task-id (:onyx/name task-map)
    :workflow (:workflow job)
    :catalog (:catalog job)
    :task task
    :flow-conditions (:flow-conditions job)
    :windows (:windows job)
    :triggers (:triggers job)
    :lifecycles (:lifecycles job)
    :metadata (:metadata job)}))

;; VirtualPeer/start
(defn create-peer-state
  [id peer-config logging-config]
  {:id id
   :type :peer
   ;;:group-id group-id
   :replica (atom {})
   ;;:log log
   ;;:messenger-group
   ;;:monitoring monitoring
   :opts peer-config
   ;;:outbox-ch outbox-ch ;CompletionHandler
   :kill-ch (async/promise-chan)
   ;;:group-ch
   :logging-config logging-config
   :peer-site (messenger/get-peer-site peer-config)})

;; log.commands.common/start-new-lifecycle
(defn create-task-state
  [job task]
  {:job-id (get-in job [:metadata :job-id])
   :task-id (:id task)
   :task-kill-ch (async/promise-chan)
   ;;:peer-site
   ;;:seal-ch
   ;;:kill-ch
   ;;; scheduler-type assoc'd into lifecycle component
   })

(defn onyx-lambda-task
  [peer-config job task task-map lambda-request]
  (let [monitoring (extensions/monitoring-agent (or (:monitoring-config peer-config)
                                                    {:monitoring :no-op}))
        logging-config (logging-config/logging-configuration peer-config)
        peer-id (-> lambda-request :context :invoked-fn-arn)
        peer-state (create-peer-state peer-id peer-config logging-config)
        task-state (create-task-state job task)] 
    (map->OnyxLambdaSystem
     {:lambda-request lambda-request
      :monitoring monitoring
      :logging-config logging-config
      :log (lambda-log peer-config)
      :completion-handler (completion-handler lambda-request)
      :task-information (create-task-information peer-id job task task-map)
      :task-monitoring (component/using monitoring [:logging-config :task-information])
      :messenger
      (component/using
       (messenger/build-messenger peer-config
                                  (messenger/build-messenger-group peer-config) peer-id)
                                  [:lambda-request])
      :task-lifecycle
      (component/using
       (lambda.task-lifecycle/task-lifecycle peer-state task-state task-map)
       [:task-information
        :task-monitoring
        :messenger
        :log
        :completion-handler])})))
