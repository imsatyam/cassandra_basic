# cassandra_basic
Demonstrates the way to use cassandra with java.

# Basic Client
Basic client uses cassandra-driver-core 3.0.0 version provided by DataStax. 
[SimpleClient](https://github.com/imsatyam/cassandra_basic/blob/master/src/main/java/com/satyam/learning/cassandra/demo/simple/SimpleClient.java) class demonstrates the usage of this API to interact with Cassandra.

# Spring Client
Spring-data-cassandra API is yet to support the 3.x version. [BookRepositoryClient](https://github.com/imsatyam/cassandra_basic/blob/master/src/main/java/com/satyam/learning/cassandra/demo/spring/client/BookRepositoryClient.java) works with an older version (2.1.x) of Cassandra and cassandra-driver-core. 
In order to use this client, please ensure that the cassandra version installed on your system, datastax driver version in pom.xml and the Spring-data-cassandra version are in synch.


Happy Coding!
