(ns onyx.lambda.extensions)

(defmulti parse-arn (fn [event-source lambda-request] event-source))
