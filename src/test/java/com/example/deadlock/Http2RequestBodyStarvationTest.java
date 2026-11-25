package com.example.deadlock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import static org.assertj.core.api.Assertions.assertThat;

/// More HTTP/2 uploads with an incomplete body than Spring Boot's
/// `VirtualThreadPool` allows concurrent tasks (`server.jetty.threads.max`,
/// 200), then a plain request on another connection. The same steps as
/// `trigger/TriggerDeadlock.java`.
class Http2RequestBodyStarvationTest {

    private static final int UPLOADS = 300;
    /// Jetty accepts at most 128 concurrent requests per HTTP/2 connection.
    private static final int CONNECTIONS = 4;
    private static final Duration UPLOAD_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HELLO_TIMEOUT = Duration.ofSeconds(3);

    /// With Spring Boot's default setup, the server stops answering while the
    /// bodies are incomplete, and for good once the client gives up on them.
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "server.shutdown=immediate")
    class WithSpringBootDefaults extends Scenario {
    }

    /// With a VirtualThreadPool without a limit, the server keeps answering.
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"server.shutdown=immediate", "reproducer.workaround=true"})
    class WithWorkaround extends Scenario {
    }

    /// Each test gets a fresh server, as a hung one stays hung. A hung server
    /// does not shut down gracefully either.
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    abstract static class Scenario {

        private final List<HttpClient> clients = new ArrayList<>();

        private String baseUri;

        @BeforeEach
        void setUp(@LocalServerPort int port) {
            baseUri = "http://localhost:%d".formatted(port);
        }

        @AfterEach
        void tearDown() {
            clients.forEach(HttpClient::shutdownNow);
        }

        /// Fails with Spring Boot's defaults.
        @Test
        void should_answer_while_request_bodies_are_incomplete() throws Exception {
            // given
            startUploads();

            // when
            String hello = hello();

            // then
            assertThat(hello).startsWith("200");
        }

        /// Passes with Spring Boot's defaults too: the server only waited.
        @Test
        void should_answer_after_the_client_sent_the_rest_of_the_bodies() throws Exception {
            // given
            List<Upload> uploads = startUploads();

            // when
            uploads.forEach(upload -> upload.body().sendRest());

            // then
            assertThat(uploads).allMatch(Upload::succeeded);
            assertThat(hello()).startsWith("200");
        }

        /// Fails with Spring Boot's defaults: the server never answers again.
        @Test
        void should_answer_after_the_client_gave_up_on_the_uploads() throws Exception {
            // given
            List<Upload> uploads = startUploads();

            // when
            assertThat(uploads).allMatch(Upload::timedOut);
            Thread.sleep(1_000);

            // then
            assertThat(hello()).startsWith("200");
        }

        /// Starts [#UPLOADS] POST /upload requests, spread over [#CONNECTIONS]
        /// HTTP/2 connections, and sends the first part of each body.
        private List<Upload> startUploads() throws Exception {
            // One HttpClient keeps all its HTTP/2 requests on one connection.
            // Its first request sets up that connection, so do that for all of
            // them before the server stops answering.
            for (int c = 0; c < CONNECTIONS; c++) {
                HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
                client.send(HttpRequest.newBuilder(URI.create(baseUri + "/hello")).build(), HttpResponse.BodyHandlers.discarding());
                clients.add(client);
            }

            List<Upload> uploads = new ArrayList<>();
            for (HttpClient client : clients) {
                for (int i = 0; i < UPLOADS / CONNECTIONS; i++) {
                    PartialBody body = new PartialBody();
                    HttpRequest request = HttpRequest.newBuilder(URI.create(baseUri + "/upload"))
                            .POST(body)
                            .timeout(UPLOAD_TIMEOUT)
                            .build();
                    uploads.add(new Upload(body, client.sendAsync(request, HttpResponse.BodyHandlers.ofString())));
                    body.sendFirst();
                }
            }
            Thread.sleep(1_000);
            return uploads;
        }

        /// GET /hello over HTTP/1.1, on a new connection.
        private String hello() throws Exception {
            try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
                HttpRequest request = HttpRequest.newBuilder(URI.create(baseUri + "/hello")).timeout(HELLO_TIMEOUT).build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                return response.statusCode() + " " + response.body();
            } catch (HttpTimeoutException e) {
                return "no answer within " + HELLO_TIMEOUT.toSeconds() + " s";
            }
        }
    }

    record Upload(PartialBody body, CompletableFuture<HttpResponse<String>> response) {

        /// Waits for the upload to end, and tells whether the client gave up on it.
        boolean timedOut() {
            return response.handle((_, failure) -> failure != null && failure.getCause() instanceof HttpTimeoutException).join();
        }

        /// Waits for the upload to end, and tells whether the server accepted it.
        boolean succeeded() {
            return response.handle((r, failure) -> failure == null && r.statusCode() == 200).join();
        }
    }

    /// A request body of [#SIZE] bytes, of which the client sends the first
    /// [#FIRST] bytes on [#sendFirst()] and the rest on [#sendRest()].
    static final class PartialBody implements HttpRequest.BodyPublisher {

        static final int SIZE = 1024;
        static final int FIRST = 16;

        private final CompletableFuture<Flow.Subscriber<? super ByteBuffer>> subscriber = new CompletableFuture<>();

        @Override
        public long contentLength() {
            return SIZE;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> s) {
            s.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    // The bytes are sent by sendFirst and sendRest, not on demand.
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.complete(s);
        }

        void sendFirst() {
            subscriber.thenAccept(s -> s.onNext(ByteBuffer.wrap(new byte[FIRST])));
        }

        void sendRest() {
            subscriber.thenAccept(s -> {
                s.onNext(ByteBuffer.wrap(new byte[SIZE - FIRST]));
                s.onComplete();
            });
        }
    }
}
