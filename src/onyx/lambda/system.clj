(ns onyx.lambda.system
  (:require [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as messenger]))

;; VirtualPeer/start
(defn create-peer-state
  [id group-id log monitoring outbox-ch peer-config logging-config]
  {:id id
   :type :peer
   :group-id group-id
   :replica (atom {})
   :log log
   ;;:messenger-group
   :monitoring monitoring
   :opts peer-config
   :outbox-ch outbox-ch
   ;;:kill-ch kill-ch
   ;;:group-ch
   :logging-config logging-config
   :peer-site (messenger/get-peer-site peer-config)})

;; log.commands.common/start-new-lifecycle
(defn create-task-state
  []
  {:job-id job-id
   :task-id task-id
   ;;:peer-site
   ;;:seal-ch
   ;;:kill-ch
   ;;:task-kill-ch
   ;;; scheduler-type assoc'd into lifecycle component
   })


(defrecord OnyxLambdaTask [peer-site peer-state task-state]
  component/Lifecycle
  (start [component]
    (component/start-system component [:task-lifecycle
                                       :task-information
                                       :task-monitoring
                                       :messenger]))
  (stop [component]
    (component/stop-system component [:task-lifecycle
                                      :task-information
                                      :task-monitoring
                                      :messenger])))

(defn lambda-log [peer-config])

(defn create-task-information [peer-state task-state job task-map])

(defn onyx-lambda-task
  [peer-config job task-map lambda-request]
  (let [monitoring (extensions/monitoring-agent (or (:monitoring-config peer-config)
                                                    {:monitoring :no-op}))
        logging-config (logging-config/logging-configuration peer-config)
        peer-state (create-peer-state ...)
        task-state (create-task-state ...)] 
    (map->OnyxLambdaTask
     {:lambda-request lambda-request
      :monitoring monitoring
      :logging-config logging-config
      :log (component/using (lambda-log peer-config) [:monitoring])
      :peer-state peer-state
      :task-state task-state
      :task-information (create-task-information peer-state task-state job task-map)
      :task-monitoring (component/using monitoring [:logging-config :task-information])
      :messenger (component/using (messenger/build-messenger peer-config
                                                             (messenger/build-messenger-group peer-config)
                                                             (:id peer-state))
                                  [:lambda-request])
      :task-lifecycle (component/using (lambda.task-lifecycle/task-lifecycle peer-state task-state)
                                       [:task-information :task-monitoring :messenger])})))
