(ns onyx.lambda.api
  (:require [com.stuartsierra.component :as component]
            [onyx.static.planning :as planning]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.lambda.system :as lambda.system]
            [onyx.lambda.extensions :as lambda.extensions]
            [onyx.lambda.lambda-request.dynamodb-update]
            [onyx.lambda.lambda-request.scheduled-event]))



(defn inject-lambda-request-lifecycle [{:keys [lifecycles] :as job} lambda-request]
  ;; todo add only to :input tasks
  (let [lambda-request-lifecycle
        {:lifecycle/task :all
         :lifecycle/calls :onyx.plugin.lambda/lifecycle-calls
         :lambda/lambda-request lambda-request}]
    (update-in job [:lifecycles] #(into [lambda-request-lifecycle] %))))

(defn match-task
  [{:keys [lambda/known-event-sources] :as peer-config} catalog lambda-request]
  (when-let [arn (some #(lambda.extensions/lambda-request-arn % lambda-request)
                  known-event-sources)]
    (some (fn [task-map]
            (when (= arn (or (:lambda/event-source-arn task-map)
                             (:lambda/stream-arn task-map)))
              task-map))
          catalog)))

(defn lambda-task
  [peer-config {:keys [catalog workflow] :as job} lambda-request]
  (let [job-id (-> lambda-request :context :aws-request-id)
        job (-> job
                (assoc-in [:metadata :job-id] job-id)
                (inject-lambda-request-lifecycle lambda-request))
        tasks (planning/discover-tasks catalog workflow)]
    (when-let [task-map (match-task peer-config catalog lambda-request)]
      (let [task (some #(when (= (:onyx/name task-map) (:name %)) %) tasks)]
        (component/start (lambda.system/onyx-lambda-task peer-config
                                                         job
                                                         task
                                                         task-map
                                                         lambda-request))))))
