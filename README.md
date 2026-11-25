# Jetty with virtual threads stops serving when HTTP/2 request bodies are incomplete

A plain Spring Boot 4.2.0 application on Jetty 12.1 with
`spring.threads.virtual.enabled=true` stops serving requests, of any protocol,
once about 200 HTTP/2 requests at the same time wait for the rest of their
body. If the clients then give up on those requests, for instance because of a
timeout, the server stays hung for good.

## Cause

With virtual threads enabled, `JettyVirtualThreadsWebServerFactoryCustomizer`
gives Jetty a `VirtualThreadPool`. That pool limits the number of concurrent
tasks with one semaphore (`server.jetty.threads.max`, default 200, see
[spring-boot#48982](https://github.com/spring-projects/spring-boot/issues/48982)),
for every task: the application's handlers and Jetty's own work alike.

A handler that reads a request body (here `request.getInputStream()`, but any
`@RequestBody` does the same) blocks while the body has not arrived, and keeps
its permit. With HTTP/2, many such requests share a few connections. Once the
blocked handlers hold all permits, every other task waits for one: new
requests, on any connection and protocol, and Jetty's own HTTP/2 work. A thread
dump shows about 200 handlers in `HttpInput.read` and the other tasks in
`Semaphore.acquire` in `VirtualThreadPool.execute`.

If the rest of the bodies arrives, the server recovers: Jetty delivers body
data on its selector threads, which already hold a permit. If the client gives
up instead, whether it cancels the requests or closes the connections, it
never recovers. Jetty then fails each request with a task that wakes up its
blocked handler, and runs that task through `ThreadPool.executeImmediately`,
because "there may be no threads available to run it" (comment in
`HTTP2ServerConnection.onStreamFailure`). For a `VirtualThreadPool`,
`tryExecute` starts a virtual thread that waits for a permit like any other
task, so the task that would free the permits waits for one itself.

A higher `server.jetty.threads.max` only takes more uploads to reach. The same
happens with a `QueuedThreadPool` whose virtual threads executor is a
`VirtualThreadPool` with a limit: that `QueuedThreadPool` reserves no platform
threads, so the task goes to the limited executor too.

Over HTTP/1.1 this needs 200 slow clients with a connection each; over HTTP/2 a
few connections are enough, for instance a proxy or a service that multiplexes
many uploads.

The pool was introduced with
[spring-boot#47690](https://github.com/spring-projects/spring-boot/issues/47690)
(3.4.11, 3.5.7). [spring-boot#50929](https://github.com/spring-projects/spring-boot/issues/50929)
describes another hang of the same setup.

## Workaround

The same `VirtualThreadPool`, without a limit on the number of concurrent
tasks, like Tomcat with virtual threads:

```java
@Bean
WebServerFactoryCustomizer<JettyServletWebServerFactory> virtualThreadPoolWithoutLimit() {
    return factory -> {
        VirtualThreadPool threadPool = new VirtualThreadPool(0);
        threadPool.setName("jetty-vt-");
        factory.setThreadPool(threadPool);
    };
}
```

Spring Boot never passes 0: `server.jetty.threads.max=0` makes it use
`new VirtualThreadPool()`, which is limited to 200 again.

The customizer has no order, so it runs after Spring Boot's and replaces its
pool. In this project it is enabled with `reproducer.workaround=true`.

## Reproducing

Needs JDK 25 (`java` and `jcmd`) on the `PATH`.

```shell
./reproduce.sh give-up     # the client gives up on the uploads when their timeout expires
./reproduce.sh send-rest   # the client sends the rest of the bodies
```

The script builds the application, starts it twice (Spring Boot's defaults and
the workaround) and runs `trigger/TriggerDeadlock.java` against each:

1. `GET /hello` over HTTP/1.1;
2. start 300 uploads over 4 HTTP/2 connections (h2c), each sending 16 of the
   1024 bytes of its body;
3. `GET /hello` again;
4. `give-up`: wait until the client gives up on the uploads after their timeout
   of 10 s; `send-rest`: send the rest of the bodies;
5. `GET /hello` for 20 s.

It takes a thread dump at the end. Output with `give-up`:

```
=================== spring-boot-defaults (reproducer.workaround=false, client: give-up)
1. GET /hello: 200 hello from VirtualThread[#49,jetty-2]/runnable@ForkJoinPool-1-worker-7
2. started 300 uploads, each sent 16 of its 1024 bytes
3. GET /hello: no answer within 3 s
4. the client gave up on 300 of the 300 uploads after their timeout of 10 s
5.  0 s later, GET /hello: no answer within 3 s
5. 10 s later, GET /hello: no answer within 3 s
5. 20 s later, GET /hello: no answer within 3 s
RESULT: the server hangs, also after the client gave up on the uploads
thread dump at the end (build/reproduce/spring-boot-defaults-threads.txt):
  193 handlers blocked reading a request body
  275 tasks waiting for a permit of Spring Boot's VirtualThreadPool

=================== workaround (reproducer.workaround=true, client: give-up)
...
3. GET /hello: 200 hello from VirtualThread[#65,jetty-vt-9]/runnable@ForkJoinPool-1-worker-2
...
5. 20 s later, GET /hello: 200 hello from VirtualThread[#405,jetty-vt-346]/runnable@ForkJoinPool-1-worker-9
RESULT: the server answers
```

With `send-rest`, the server with Spring Boot's defaults also does not answer
`GET /hello` while the bodies are incomplete, but recovers once they arrive.

The same scenarios as tests (`Http2RequestBodyStarvationTest`). With Spring
Boot's defaults, the tests that expect an answer while the bodies are
incomplete and after the client gave up fail; with the workaround all pass:

```shell
./gradlew test
```

The trigger also runs on its own against any server:

```shell
java trigger/TriggerDeadlock.java http://localhost:8080 [give-up|send-rest]
```
