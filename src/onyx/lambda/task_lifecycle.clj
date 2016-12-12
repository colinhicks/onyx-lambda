(ns onyx.lambda.task-lifecycle
  (:require [clojure.core.async :as async]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [com.stuartsierra.component :as component]
            [onyx.peer.operation :as operation]
            [onyx.peer.function :as function]
            [onyx.peer.task-compile :as task-compile]
            [onyx.log.entry :as entry]
            [onyx.windowing.window-compile :as window-compile]
            [onyx.peer.task-lifecycle :as peer.task-lifecycle]
            [onyx.plugin.onyx-plugin :as onyx-plugin]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count! map->EventState ->EventState]]
            [onyx.lifecycles.lifecycle-invoke :as lifecycle-invoke]
            [onyx.static.logging :as logger]))



(defn transition [state]
    (case (:lifecycle state)
;;      :poll-recover identity
;;      :recovering identity
;;      :start-processing identity
;;      :input-poll-barriers identity
;;      :prepare-emit-barriers identity
;;      :emit-barriers identity
;;      :poll-acks identity
;;      :before-batch identity
;;      :read-batch identity
;;      :apply-fn identity
;;      :build-new-segments identity
;;      :assign-windows identity
;;      :prepare-batch identity
;;      :write-batch identity
;;      :after-batch identity
;;      :prepare-ack-barriers identity
;;      :ack-barriers identity
      (peer.task-lifecycle/transition state)))

(defn task-alive? [event]
  #_(first (alts!! [(:task-kill-ch event) (:kill-ch event)] :default true))
  true)

(defn next-state [prev-state]
  (if (task-alive? (:init-event prev-state))
    (loop [state prev-state]
      ;;(print-state state)
      (let [new-state (-> state
                          transition
                          peer.task-lifecycle/next-lifecycle)]
        (if (or (= :blocked (:state new-state))
                (= :start-processing (:lifecycle new-state)))
          (do
            (info "Task dropping out" (:task-type (:event new-state)))
            new-state)
          (recur new-state))))
    (assoc prev-state :lifecycle :killed)))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [init-state ex-f]
  (try
    (assert (:event init-state))
    (let [{:keys [task-kill-ch kill-ch task-information replica-atom opts state]} (:event init-state)] 
      (loop [prev-state init-state]
        ;; TODO add here :emit-barriers, emit-ack-barriers?
        ;(println "Iteration " (:state prev-state))
        (info "Task Dropping back in " (:task-type (:event init-state)))
        (let [state (next-state prev-state)]
          (assert (empty? (.__extmap state)) 
                  (str "Ext-map for state record should be empty at start. Contains: " 
                       (keys (.__extmap state))))
          (if-not (= :killed (:lifecycle state)) 
            (recur state)
            prev-state))))
   (catch Throwable e
     (ex-f e)
     init-state)))

(defn start-task-lifecycle! [initial-state ex-f]
  (run-task-lifecycle initial-state ex-f))

(defn handle-exception [task-info e outbox-ch job-id]
  (let [data (ex-data e)
        ;; Default to original exception if Onyx didn't wrap the original exception
        inner (or (.getCause ^Throwable e) e)]
    (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
        (let [entry (entry/create-log-entry :kill-job {:job job-id})]
          (async/>!! outbox-ch entry)))))

(defn start-lifecycle
  [id job-id task-id 
   task task-map workflow catalog flow-conditions windows triggers lifecycles metadata
   task-monitoring messenger]
  (let [filtered-windows []
        window-ids #{}
        filtered-triggers []
        replica nil
        coordinator nil
        log-prefix nil
        log nil
        opts {}
        task-information nil
        outbox-ch nil
        group-ch nil
        task-kill-ch nil
        kill-ch nil
        pipeline-data (map->Event
                       {:id id
                        :job-id job-id
                        :task-id task-id
                        :slot-id nil
                        :task (:name task)
                        :catalog catalog
                        :workflow workflow
                        :windows filtered-windows
                        :triggers filtered-triggers
                        :flow-conditions flow-conditions
                        :lifecycles lifecycles
                        :metadata metadata
                        :task-map task-map
                        :serialized-task task
                        :log log
                        :monitoring task-monitoring
                        :task-information task-information
                        :outbox-ch outbox-ch
                        :group-ch group-ch
                        :task-kill-ch task-kill-ch
                        :kill-ch kill-ch
                        ;;Rename to peer-config
                        :peer-opts opts
                        :fn (operation/resolve-task-fn task-map)
                        :replica-atom replica
                        :log-prefix log-prefix})

        pipeline-data (->> pipeline-data
                           task-compile/task-params->event-map
                           task-compile/flow-conditions->event-map
                           task-compile/lifecycles->event-map
                           task-compile/task->event-map)

        ex-f (fn [e] (handle-exception e))
        event (lifecycle-invoke/invoke-before-task-start pipeline-data)
        initial-state (map->EventState
                       {:lifecycle :poll-recover
                        :state :runnable
                        :replica replica
                        :messenger messenger
                        :coordinator coordinator
                        :pipeline (peer.task-lifecycle/build-pipeline task-map event)
                        :init-event event
                        :event event})]
    (start-task-lifecycle! initial-state ex-f)))


(defrecord LambdaTaskLifecycle
    [id log messenger job-id task-id replica #_group-ch log-prefix #_kill-ch outbox-ch #_seal-ch 
     #_completion-ch #_peer-group opts task-kill-ch scheduler-event task-monitoring task-information
     completion-handler task-map]
  component/Lifecycle
  (start [component]
    (let [{:keys [workflow catalog task flow-conditions
                  windows triggers lifecycles metadata]} task-information
          log-prefix (logger/log-prefix task-information)
          filtered-windows (vec (window-compile/filter-windows windows (:name task)))
          window-ids (set (map :window/id filtered-windows))
          filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
          coordinator nil
          pipeline-data (map->Event
                         {:id id
                          :job-id job-id
                          :task-id task-id
                          :slot-id nil ;;
                          :task (:name task)
                          :catalog catalog
                          :workflow workflow
                          :windows filtered-windows
                          :triggers filtered-triggers
                          :flow-conditions flow-conditions
                          :lifecycles lifecycles
                          :metadata metadata
                          :task-map task-map
                          :serialized-task task
                          :log log
                          :monitoring task-monitoring
                          :task-information task-information
                          :outbox-ch outbox-ch
                          :group-ch nil ;;
                          :task-kill-ch task-kill-ch
                          :kill-ch nil ;;
                          :peer-opts opts
                          :fn (operation/resolve-task-fn task-map)
                          :replica-atom replica
                          :log-prefix log-prefix})

          pipeline-data (->> pipeline-data
                           task-compile/task-params->event-map
                           task-compile/flow-conditions->event-map
                           task-compile/lifecycles->event-map
                           task-compile/task->event-map)

          ex-f (fn [e] (handle-exception task-information e outbox-ch job-id))
          event (lifecycle-invoke/invoke-before-task-start pipeline-data)
          initial-state (map->EventState
                         {:lifecycle :poll-recover
                          :state :runnable
                          :replica replica
                          :messenger messenger
                          :coordinator coordinator ;;nil
                          :pipeline (peer.task-lifecycle/build-pipeline task-map event)
                          :barriers {}
                          :exhausted? false
                          :init-event event
                          :event event})]
      (assoc component
                :event event
                :state initial-state
                :log-prefix log-prefix
                :task-information task-information
                :holder (atom nil)
                :task-kill-ch task-kill-ch
                ;;:kill-ch kill-ch
                ;;:task-lifecycle-ch task-lifecycle-ch
                )))

  (stop [component]
    component))

(defn task-lifecycle [peer-state task-state task-map]
  (map->LambdaTaskLifecycle (merge peer-state
                                   task-state
                                   {:task-map task-map})))
