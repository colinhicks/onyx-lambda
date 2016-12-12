(ns onyx.lambda.extensions)

(defmulti lambda-request-arn (fn [event-source lambda-request] event-source))

(defmulti lambda-request-segments (fn [event-source lambda-request] event-source))
