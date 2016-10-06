(defproject net.colinhicks/onyx-lambda "0.0.1-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [com.amazonaws/aws-lambda-java-core "1.1.0"]
                 [org.onyxplatform/onyx "0.9.8-SNAPSHOT"
                  :exclusions [[org.apache.curator/curator-framework]
                               [org.apache.curator/curator-test]
                               [org.apache.zookeeper/zookeeper]
                               [org.apache.bookkeeper/bookkeeper-server]
                               [org.btrplace/scheduler-api]
                               [org.btrplace/scheduler-choco]]]])

