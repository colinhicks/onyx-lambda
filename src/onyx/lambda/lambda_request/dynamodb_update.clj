(ns onyx.lambda.lambda-request.dynamodb-update
  (:require [onyx.lambda.extensions :as lambda.extensions]))


(defmethod lambda.extensions/lambda-request-arn :dynamodb-update
  [_ lambda-request]
  (->> lambda-request :body :Records (map :eventSourceARN) first))

(defmethod lambda.extensions/lambda-request-segments :dynamodb-update
  [_ lambda-request]
  ;; todo clj map
  (->> lambda-request :body :Records))
