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

/// Makes the server stop answering, and shows whether it recovers. Run with:
///
///     java trigger/TriggerDeadlock.java [base URL] [give-up|send-rest]
///
/// 1. Checks that GET /hello is answered.
/// 2. Starts 300 uploads over HTTP/2 that send only the first part of their
///    body. Each upload's handler blocks, waiting for the rest.
/// 3. Checks GET /hello again: with Spring Boot's defaults the server no
///    longer answers, because it lets Jetty run at most 200 tasks at a time.
/// 4. Ends the uploads, in one of two ways:
///    - give-up: the client gives up on the uploads when their timeout
///      expires, like any client with a timeout. It cancels each upload.
///    - send-rest: the client sends the rest of the bodies.
/// 5. Checks GET /hello for 20 s. After send-rest the server answers again.
///    After give-up it never does with Spring Boot's defaults, although
///    nobody is uploading any more.
///
/// Exits with 1 if the server no longer answers at the end.
public class TriggerDeadlock {

    static final int UPLOADS = 300;
    /// Jetty accepts at most 128 concurrent requests per HTTP/2 connection.
    static final int CONNECTIONS = 4;
    static final Duration UPLOAD_TIMEOUT = Duration.ofSeconds(10);
    static final Duration HELLO_TIMEOUT = Duration.ofSeconds(3);

    public static void main(String[] args) throws Exception {
        String baseUri = args.length > 0 ? args[0] : "http://localhost:8080";
        boolean giveUp = !(args.length > 1 && args[1].equals("send-rest"));

        System.out.println("1. GET /hello: " + hello(baseUri));

        List<Upload> uploads = startUploads(baseUri);
        Thread.sleep(1_000);
        System.out.printf("2. started %d uploads, each sent %d of its %d bytes%n", UPLOADS, PartialBody.FIRST, PartialBody.SIZE);

        System.out.println("3. GET /hello: " + hello(baseUri));

        if (giveUp) {
            long gaveUp = uploads.stream().filter(Upload::timedOut).count();
            System.out.printf("4. the client gave up on %d of the %d uploads after their timeout of %d s%n",
                    gaveUp, UPLOADS, UPLOAD_TIMEOUT.toSeconds());
        } else {
            uploads.forEach(upload -> upload.body().sendRest());
            long completed = uploads.stream().filter(Upload::succeeded).count();
            System.out.printf("4. sent the rest of the bodies: %d of the %d uploads completed%n", completed, UPLOADS);
        }

        String last = "";
        for (int seconds = 0; seconds <= 20; seconds += 10) {
            Thread.sleep(seconds == 0 ? 1_000 : 10_000);
            last = hello(baseUri);
            System.out.printf("5. %2d s later, GET /hello: %s%n", seconds, last);
        }

        boolean answers = last.startsWith("200");
        System.out.println(answers ? "RESULT: the server answers"
                : giveUp ? "RESULT: the server hangs, also after the client gave up on the uploads"
                : "RESULT: the server hangs, also after the uploads completed");
        System.exit(answers ? 0 : 1);
    }

    /// Starts [#UPLOADS] POST /upload requests, spread over [#CONNECTIONS]
    /// HTTP/2 connections, and sends the first part of each body.
    static List<Upload> startUploads(String baseUri) throws Exception {
        // One HttpClient keeps all its HTTP/2 requests on one connection. Its
        // first request sets up that connection, so do that for all of them
        // before the server stops answering.
        List<HttpClient> clients = new ArrayList<>();
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
        return uploads;
    }

    /// GET /hello over HTTP/1.1, on a new connection.
    static String hello(String baseUri) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(baseUri + "/hello")).timeout(HELLO_TIMEOUT).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() + " " + response.body();
        } catch (HttpTimeoutException e) {
            return "no answer within " + HELLO_TIMEOUT.toSeconds() + " s";
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
