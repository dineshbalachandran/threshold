A Flink stream processing application using Scala and SBT. The application generates 'threshold' breach events by matching incoming events against a defined threshold (number of events within a period of time).
- The externally configured 'threshold definition' is loaded by the application. It supports multiple levels of threshold.
- Flink state management and tumbling time windows are used to 'detect' a threshold breach
- The application is flexible in that it supports input and output event stream as well as the threshold definitions from a variety of source types i.e file based, kinesis, database. Other source types can be added. 
For e.g. the unit tests use file and in memory database source types, while in production it will be sql database and kinesis or kafka streams.  

Unit tests use JUnit and ScalaTest.



