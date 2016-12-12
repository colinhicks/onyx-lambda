(ns onyx.lambda.api-test
  (:require [onyx.lambda.api :as api]
            [onyx.lambda.lambda-request :as lambda-request]
            [clojure.test :refer [deftest is testing]]))


(defn some-transform [segment]
  segment)

(deftest lambda-task-api
  (let [lambda-request
        (lambda-request/map->LambdaRequest
         {:body {:source "aws.events"
                 :resources ["arn:aws:events:us-east1:123456:rule/my-sched"]}
          :context {:aws-request-id "aefcdg-2fda"
                    :invoked-fn-arn "arn:aws:us-east1:234123:..."}})
        peer-config {:onyx.messaging/impl :lambda
                     :lambda/known-event-sources [:scheduled-event :dynamodb-update]}
        workflow [[:a :b] [:b :c]]
        catalog
        [{:onyx/name :a
          :onyx/type :input
          :onyx/medium :lambda
          :onyx/plugin :onyx.plugin.lambda/input
          :lambda/event-source :scheduled-event
          :lambda/event-source-arn (-> lambda-request :body :resources first)
          :lambda/stream-arn "arn:aws:dynamodb:us-east1:...table/my-a"
          :lambda/table :a
          :onyx/batch-size 1
          :onyx/fn ::some-transform}
         {:onyx/name :b
          :onyx/type :function
          :lambda/stream-arn "arn:aws:dynamodb:us-east1:...table/my-b"
          :lambda/table :b
          :onyx/fn ::some-transform}
         {:onyx/name :c
          :onyx/type :output
          :onyx/plugin :onyx.plugin/todo
          :lambda/stream-arn "arn:aws:dynamodb:us-east1:...table/my-c"
          :onyx/fn ::some-transform}]
        job {:catalog catalog
             :workflow workflow}
        lambda-task (api/lambda-task peer-config job lambda-request)]
    (is (= :a
           (get-in lambda-task [:task-information :task :name])))
    
    (is (= {:lifecycle/task :all
            :lifecycle/calls :onyx.plugin.lambda/lifecycle-calls
            :lambda/lambda-request lambda-request}
           (first (get-in lambda-task [:task-information :lifecycles]))))

    (is (= lambda-request
           (get-in lambda-task [:task-lifecycle :state :init-event :lambda/lambda-request])))))
