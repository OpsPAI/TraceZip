package org.example.apache_http_client;

import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntityContainer;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.util.Random;

public class ApacheHttpExecute {
    private static final String[] urls = {
            "http://localhost:8000/1",
            "http://localhost:8000/2",
            "http://localhost:8000/3",
            "http://localhost:8000/4",
            "http://localhost:8000/5",
            "http://localhost:8000/6",
            "http://localhost:8000/7",
            "http://localhost:8000/8",
            "http://localhost:8000/9",
            "http://localhost:8000/a"
    };

    private static final String[] userAgents = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
            "Mozilla/5.0 (Linux; Android 9; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Mobile Safari/537.36"
    };

    private static final String[] origins = {
            "http://example.com",
            "http://anotherexample.com",
            "http://yetanotherexample.com"
    };

    private static final String[] contentTypes = {
            "application/json",
            "text/plain",
            "application/xml"
    };

    private static final String[] methods = {
            "GET",
            "POST",
            "PUT",
            "DELETE",
            "PATCH"
    };

    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            for (int i = 0; i < 10000000; i++) {
                String url = urls[random.nextInt(urls.length)];
                String method = methods[random.nextInt(methods.length)];

                switch (method) {
                    case "GET":
                        executeGet(client, url);
                        break;
                    case "POST":
                        executePost(client, url);
                        break;
                    case "PUT":
                        executePut(client, url);
                        break;
                    case "DELETE":
                        executeDelete(client, url);
                        break;
                    case "PATCH":
                        executePatch(client, url);
                        break;
                }
            }
        }
    }

    private static void executeGet(CloseableHttpClient client, String url) throws Exception {
        HttpGet request = new HttpGet(url);
        setRandomHeaders(request);
        try (CloseableHttpResponse response = client.execute(request)) {
            printResponse("GET", url, response);
        }
    }

    private static void executePost(CloseableHttpClient client, String url) throws Exception {
        HttpPost request = new HttpPost(url);
        setRandomHeaders(request);
        setRandomPayload(request);
        try (CloseableHttpResponse response = client.execute(request)) {
            printResponse("POST", url, response);
        }
    }

    private static void executePut(CloseableHttpClient client, String url) throws Exception {
        HttpPut request = new HttpPut(url);
        setRandomHeaders(request);
        setRandomPayload(request);
        try (CloseableHttpResponse response = client.execute(request)) {
            printResponse("PUT", url, response);
        }
    }

    private static void executeDelete(CloseableHttpClient client, String url) throws Exception {
        HttpDelete request = new HttpDelete(url);
        setRandomHeaders(request);
        try (CloseableHttpResponse response = client.execute(request)) {
            printResponse("DELETE", url, response);
        }
    }

    private static void executePatch(CloseableHttpClient client, String url) throws Exception {
        HttpPatch request = new HttpPatch(url);
        setRandomHeaders(request);
        setRandomPayload(request);
        try (CloseableHttpResponse response = client.execute(request)) {
            printResponse("PATCH", url, response);
        }
    }

    private static void setRandomHeaders(HttpUriRequestBase request) {
        request.setHeader("User-Agent", userAgents[random.nextInt(userAgents.length)]);
        request.setHeader("Origin", origins[random.nextInt(origins.length)]);
        if (random.nextBoolean()) {
            request.setHeader("X-Custom-Header", "CustomValue");
        }
    }

    private static void setRandomPayload(HttpEntityContainer request) {
        String contentType = contentTypes[random.nextInt(contentTypes.length)];

        String payload;
        switch (contentType) {
            case "application/json":
                payload = "{\"key\":\"value\"}";
                break;
            case "application/xml":
                payload = "<key>value</key>";
                break;
            case "text/plain":
            default:
                payload = "This is a plain text payload.";
                break;
        }
        request.setEntity(new StringEntity(payload, ContentType.parse("UTF-8")));
    }

    private static void printResponse(String method, String url, CloseableHttpResponse response) throws Exception {
        System.out.println(method + " " + url);
        System.out.println("Response Code: " + response.getCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        System.out.println("Response Body: " + responseBody);
    }
}
