# hsqldb-loom

This repo tests how HSQLDB works with virtual threads.

## Usage

```shell
# Install Java 20
# Build the jar
./mvnw clean package

# Run the test using LOCKS
java --enable-preview -Djdk.tracePinnedThreads=full -jar target/hsqldb-loom-1.0-SNAPSHOT-exec.jar LOCKS

# Run the test using MVCC
java --enable-preview -Djdk.tracePinnedThreads=full -jar target/hsqldb-loom-1.0-SNAPSHOT-exec.jar MVCC
```