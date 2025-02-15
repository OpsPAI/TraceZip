package org.example.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.example.grpc.Example;

import java.io.IOException;

public class TestServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(8080)
                .addService(new TestServiceImpl())
                .build();

        server.start();
        System.out.println("Server started...");
        server.awaitTermination();
    }
}

class TestServiceImpl extends TestServiceGrpc.TestServiceImplBase {

    @Override
    public void service1(Example.ServiceRequest request, StreamObserver<Example.ServiceResponse> responseObserver) {
        String result = "Service1: " + request.getParam1() + ", " + request.getParam2() + ", " + request.getParam3();
        Example.ServiceResponse response = Example.ServiceResponse.newBuilder().setResult(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void service2(Example.ServiceRequest request, StreamObserver<Example.ServiceResponse> responseObserver) {
        String result = "Service2: " + request.getParam1() + ", " + request.getParam2() + ", " + request.getParam3();
        Example.ServiceResponse response = Example.ServiceResponse.newBuilder().setResult(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void service3(Example.ServiceRequest request, StreamObserver<Example.ServiceResponse> responseObserver) {
        String result = "Service3: " + request.getParam1() + ", " + request.getParam2() + ", " + request.getParam3();
        Example.ServiceResponse response = Example.ServiceResponse.newBuilder().setResult(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void service4(Example.ServiceRequest request, StreamObserver<Example.ServiceResponse> responseObserver) {
        String result = "Service4: " + request.getParam1() + ", " + request.getParam2() + ", " + request.getParam3();
        Example.ServiceResponse response = Example.ServiceResponse.newBuilder().setResult(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void service5(Example.ServiceRequest request, StreamObserver<Example.ServiceResponse> responseObserver) {
        String result = "Service5: " + request.getParam1() + ", " + request.getParam2() + ", " + request.getParam3();
        Example.ServiceResponse response = Example.ServiceResponse.newBuilder().setResult(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
