# Flight App (CSE344)

To test cases folder recursively (all ``.txt`` files):

```
mvn test
```

To test a specific folder recursively or a specific file:

```
mvn test -Dtest.cases="[folder/file]"
```

To test with a specific server credential:

```
mvn test -Dcredential="[server_URL];[db_name];[username];[password]"
```

To run flight app:

```
mvn compile exec:java
```

To build jar with dependency and then run that jar:

```
mvn compile assembly:single

java -jar target/FlightApp-1.0-jar-with-dependencies.jar
```
