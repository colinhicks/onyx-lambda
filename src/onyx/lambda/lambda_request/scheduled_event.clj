(ns onyx.lambda.lambda-request.scheduled-event
  (:require [onyx.lambda.extensions :as lambda.extensions]))

(defmethod lambda.extensions/lambda-request-arn :scheduled-event
  [_ lambda-request]
  (->> lambda-request :body :resources first))

(defmethod lambda.extensions/lambda-request-segments :scheduled-event
  [_ lambda-request]
  (->> lambda-request :body vector))
