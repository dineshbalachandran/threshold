#### Threshold Breach Identifier

An Apache Flink stream processing application using *Scala and SBT*.
The application generates 'threshold' breach events by matching incoming events against a defined threshold (number of events within a period of time).
- The externally configured 'threshold definition' is loaded by the application asynchronously. It supports multiple levels of threshold.
- Flink state management and tumbling time windows are used to 'detect' a threshold breach
- The application is flexible in that it supports input and output event streams as well as the threshold definitions from a variety of source types i.e. file based, Kinesis, sql database etc. 
- It is designed to easily add other source types. For e.g. the unit tests use file and in memory database source types, while in production it can be sql database and Kinesis/Kafka streams.  

Unit tests use JUnit and ScalaTest.

###### Running app (via tests)
sbt run


