package com.example.deadlock;

import jakarta.servlet.http.HttpServletRequest;
import org.eclipse.jetty.util.thread.VirtualThreadPool;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.jetty.servlet.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@SpringBootApplication
public class DeadlockApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeadlockApplication.class, args);
    }

    @RestController
    static class Endpoints {

        @GetMapping("/hello")
        String hello() {
            return "hello from " + Thread.currentThread();
        }

        /// Reads the whole request body, like any controller that takes a
        /// `@RequestBody` does. The read blocks until the body has arrived.
        @PostMapping("/upload")
        String upload(HttpServletRequest request) throws IOException {
            return "received " + request.getInputStream().readAllBytes().length + " bytes";
        }
    }

    /// The workaround: the same `VirtualThreadPool` that Spring Boot installs,
    /// but without a limit on the number of concurrent tasks, like Tomcat with
    /// virtual threads.
    ///
    /// Any limit can make the server hang for good: once blocked handlers hold
    /// all permits, the task that would wake them up when the client cancels
    /// an upload waits for a permit as well. A higher limit only takes more
    /// uploads to reach. See the README.
    ///
    /// This customizer has no order, so it runs after Spring Boot's
    /// `JettyVirtualThreadsWebServerFactoryCustomizer` and replaces its pool.
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBooleanProperty("reproducer.workaround")
    static class WorkaroundConfiguration {

        @Bean
        WebServerFactoryCustomizer<JettyServletWebServerFactory> virtualThreadPoolWithoutLimit() {
            return factory -> {
                VirtualThreadPool threadPool = new VirtualThreadPool(0);
                threadPool.setName("jetty-vt-");
                factory.setThreadPool(threadPool);
            };
        }
    }
}
