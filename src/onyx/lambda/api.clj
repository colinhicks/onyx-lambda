(ns onyx.lambda.api
  (:require [com.stuartsierra.component :as component]
            [onyx.static.planning :as planning]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.lambda.system :as lambda.system]))

;; ;; Following copied from onyx.api, which requires excluded dependencies (e.g. ZK) ...
;; (defn ^{:no-doc true} saturation [catalog]
;;   (let [rets
;;         (reduce #(+ %1 (or (:onyx/max-peers %2)
;;                            Double/POSITIVE_INFINITY))
;;                 0
;;                 catalog)]
;;     (if (zero? rets)
;;       Double/POSITIVE_INFINITY
;;       rets)))

;; (defn ^{:no-doc true} task-saturation [catalog tasks]
;;   (into
;;    {}
;;    (map
;;     (fn [task]
;;       {(:id task)
;;        (or (:onyx/max-peers (planning/find-task catalog (:name task)))
;;            Double/POSITIVE_INFINITY)})
;;     tasks)))

;; (defn ^{:no-doc true} min-required-peers [catalog tasks]
;;   (into
;;    {}
;;    (map
;;     (fn [task]
;;       {(:id task)
;;        (or (:onyx/min-peers (planning/find-task catalog (:name task))) 1)})
;;     tasks)))

;; (defn ^{:no-doc true} required-tags [catalog tasks]
;;   (reduce
;;    (fn [result [catalog-entry task]]
;;      (if-let [tags (:onyx/required-tags catalog-entry)]
;;        (assoc result (:id task) tags)
;;        result))
;;    {}
;;    (map vector catalog tasks)))

;; (defn ^{:no-doc true} flux-policies [catalog tasks]
;;   (->> tasks
;;        (map (fn [task]
;;               (vector (:id task)
;;                       (:onyx/flux-policy (planning/find-task catalog (:name task))))))
;;        (filter second)
;;        (into {})))

;; (defn ^{:no-doc true} find-input-tasks [catalog tasks]
;;   (mapv :id (filter (fn [task]
;;                       (let [task (planning/find-task catalog (:name task))]
;;                         (= :input (:onyx/type task))))
;;                     tasks)))

;; (defn ^{:no-doc true} find-output-tasks [catalog tasks]
;;   (mapv :id (filter (fn [task]
;;                      (let [task (planning/find-task catalog (:name task))]
;;                        (= :output (:onyx/type task))))
;;                    tasks)))

;; (defn ^{:no-doc true} find-state-tasks [windows]
;;   (vec (distinct (map :window/task windows))))

;; (defn ^{:no-doc true} expand-n-peers [catalog]
;;   (mapv
;;    (fn [entry]
;;      (if-let [n (:onyx/n-peers entry)]
;;        (assoc entry :onyx/min-peers n :onyx/max-peers n)
;;        entry))
;;    catalog))

;; (defn job-entry [id config job tasks]
;;   (let [task-ids (mapv :id tasks)
;;         job (update job :catalog expand-n-peers)
;;         scheduler (:task-scheduler job)
;;         sat (saturation (:catalog job))
;;         task-saturation (task-saturation (:catalog job) tasks)
;;         min-reqs (min-required-peers (:catalog job) tasks)
;;         task-flux-policies (flux-policies (:catalog job) tasks)
;;         input-task-ids (find-input-tasks (:catalog job) tasks)
;;         output-task-ids (find-output-tasks (:catalog job) tasks)
;;         state-task-ids (find-state-tasks (:windows job))
;;         required-tags (required-tags (:catalog job) tasks)]
;;     {:id id
;;      :tasks task-ids
;;      :task-name->id (reduce (fn [result t] (assoc result (:name t) (:id t))) {} tasks)
;;      :task-scheduler scheduler
;;      :saturation sat
;;      :task-saturation task-saturation
;;      :min-required-peers min-reqs
;;      :flux-policies task-flux-policies
;;      :inputs input-task-ids
;;      :outputs output-task-ids
;;      :state state-task-ids
;;      :required-tags required-tags}))


(def lambda-request-lifecycle
  {:lifecycle/before-task-start
   (fn [event {:keys [onyx.lambda/lambda-request]}]
     {:lambda-request lambda-request})})

(defn inject-lambda-request-lifecycle [{:keys [lifecycles]} lambda-request]
  ;; todo add only to :input tasks
  (let [lambda-request-lifecycle
        [{:lifecycle/task :all
          :lifecycle/calls ::lambda-request-lifecycle
          :onyx.lambda/lambda-request lambda-request}]]
    (into [lambda-request-lifecycle] lifecycles)))

(defn match-task [catalog lambda-request]
  )

(defn run-task [peer-config job lambda-request]
  (let [job (-> job 
                (update-in [:metadata :job-id] #(or % (random-uuid))))
        id (get-in job [:metadata :job-id])
        tasks (planning/discover-tasks (:catalog job) (:workflow job))
        task-map (match-task (:catalog job) lambda-request)]
    (component/start
     (lambda.system/onyx-lambda-task peer-config job task-map lambda-request))))
