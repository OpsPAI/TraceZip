package org.example.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */

@io.grpc.stub.annotations.GrpcGenerated
public final class TestServiceGrpc {

  private TestServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "test.TestService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService1Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Service1",
      requestType = Example.ServiceRequest.class,
      responseType = Example.ServiceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService1Method() {
    io.grpc.MethodDescriptor<Example.ServiceRequest, Example.ServiceResponse> getService1Method;
    if ((getService1Method = TestServiceGrpc.getService1Method) == null) {
      synchronized (TestServiceGrpc.class) {
        if ((getService1Method = TestServiceGrpc.getService1Method) == null) {
          TestServiceGrpc.getService1Method = getService1Method =
              io.grpc.MethodDescriptor.<Example.ServiceRequest, Example.ServiceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Service1"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TestServiceMethodDescriptorSupplier("Service1"))
              .build();
        }
      }
    }
    return getService1Method;
  }

  private static volatile io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService2Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Service2",
      requestType = Example.ServiceRequest.class,
      responseType = Example.ServiceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService2Method() {
    io.grpc.MethodDescriptor<Example.ServiceRequest, Example.ServiceResponse> getService2Method;
    if ((getService2Method = TestServiceGrpc.getService2Method) == null) {
      synchronized (TestServiceGrpc.class) {
        if ((getService2Method = TestServiceGrpc.getService2Method) == null) {
          TestServiceGrpc.getService2Method = getService2Method =
              io.grpc.MethodDescriptor.<Example.ServiceRequest, Example.ServiceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Service2"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TestServiceMethodDescriptorSupplier("Service2"))
              .build();
        }
      }
    }
    return getService2Method;
  }

  private static volatile io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService3Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Service3",
      requestType = Example.ServiceRequest.class,
      responseType = Example.ServiceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService3Method() {
    io.grpc.MethodDescriptor<Example.ServiceRequest, Example.ServiceResponse> getService3Method;
    if ((getService3Method = TestServiceGrpc.getService3Method) == null) {
      synchronized (TestServiceGrpc.class) {
        if ((getService3Method = TestServiceGrpc.getService3Method) == null) {
          TestServiceGrpc.getService3Method = getService3Method =
              io.grpc.MethodDescriptor.<Example.ServiceRequest, Example.ServiceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Service3"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TestServiceMethodDescriptorSupplier("Service3"))
              .build();
        }
      }
    }
    return getService3Method;
  }

  private static volatile io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService4Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Service4",
      requestType = Example.ServiceRequest.class,
      responseType = Example.ServiceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService4Method() {
    io.grpc.MethodDescriptor<Example.ServiceRequest, Example.ServiceResponse> getService4Method;
    if ((getService4Method = TestServiceGrpc.getService4Method) == null) {
      synchronized (TestServiceGrpc.class) {
        if ((getService4Method = TestServiceGrpc.getService4Method) == null) {
          TestServiceGrpc.getService4Method = getService4Method =
              io.grpc.MethodDescriptor.<Example.ServiceRequest, Example.ServiceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Service4"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TestServiceMethodDescriptorSupplier("Service4"))
              .build();
        }
      }
    }
    return getService4Method;
  }

  private static volatile io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService5Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Service5",
      requestType = Example.ServiceRequest.class,
      responseType = Example.ServiceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Example.ServiceRequest,
      Example.ServiceResponse> getService5Method() {
    io.grpc.MethodDescriptor<Example.ServiceRequest, Example.ServiceResponse> getService5Method;
    if ((getService5Method = TestServiceGrpc.getService5Method) == null) {
      synchronized (TestServiceGrpc.class) {
        if ((getService5Method = TestServiceGrpc.getService5Method) == null) {
          TestServiceGrpc.getService5Method = getService5Method =
              io.grpc.MethodDescriptor.<Example.ServiceRequest, Example.ServiceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Service5"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Example.ServiceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TestServiceMethodDescriptorSupplier("Service5"))
              .build();
        }
      }
    }
    return getService5Method;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TestServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TestServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TestServiceStub>() {
        @java.lang.Override
        public TestServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TestServiceStub(channel, callOptions);
        }
      };
    return TestServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TestServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TestServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TestServiceBlockingStub>() {
        @java.lang.Override
        public TestServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TestServiceBlockingStub(channel, callOptions);
        }
      };
    return TestServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TestServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TestServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TestServiceFutureStub>() {
        @java.lang.Override
        public TestServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TestServiceFutureStub(channel, callOptions);
        }
      };
    return TestServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void service1(Example.ServiceRequest request,
                          io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getService1Method(), responseObserver);
    }

    /**
     */
    default void service2(Example.ServiceRequest request,
                          io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getService2Method(), responseObserver);
    }

    /**
     */
    default void service3(Example.ServiceRequest request,
                          io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getService3Method(), responseObserver);
    }

    /**
     */
    default void service4(Example.ServiceRequest request,
                          io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getService4Method(), responseObserver);
    }

    /**
     */
    default void service5(Example.ServiceRequest request,
                          io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getService5Method(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TestService.
   */
  public static abstract class TestServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TestServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TestService.
   */
  public static final class TestServiceStub
      extends io.grpc.stub.AbstractAsyncStub<TestServiceStub> {
    private TestServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TestServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TestServiceStub(channel, callOptions);
    }

    /**
     */
    public void service1(Example.ServiceRequest request,
                         io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getService1Method(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void service2(Example.ServiceRequest request,
                         io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getService2Method(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void service3(Example.ServiceRequest request,
                         io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getService3Method(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void service4(Example.ServiceRequest request,
                         io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getService4Method(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void service5(Example.ServiceRequest request,
                         io.grpc.stub.StreamObserver<Example.ServiceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getService5Method(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TestService.
   */
  public static final class TestServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TestServiceBlockingStub> {
    private TestServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TestServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TestServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public Example.ServiceResponse service1(Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getService1Method(), getCallOptions(), request);
    }

    /**
     */
    public Example.ServiceResponse service2(Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getService2Method(), getCallOptions(), request);
    }

    /**
     */
    public Example.ServiceResponse service3(Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getService3Method(), getCallOptions(), request);
    }

    /**
     */
    public Example.ServiceResponse service4(Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getService4Method(), getCallOptions(), request);
    }

    /**
     */
    public Example.ServiceResponse service5(Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getService5Method(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TestService.
   */
  public static final class TestServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<TestServiceFutureStub> {
    private TestServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TestServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TestServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<Example.ServiceResponse> service1(
        Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getService1Method(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<Example.ServiceResponse> service2(
        Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getService2Method(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<Example.ServiceResponse> service3(
        Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getService3Method(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<Example.ServiceResponse> service4(
        Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getService4Method(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<Example.ServiceResponse> service5(
        Example.ServiceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getService5Method(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SERVICE1 = 0;
  private static final int METHODID_SERVICE2 = 1;
  private static final int METHODID_SERVICE3 = 2;
  private static final int METHODID_SERVICE4 = 3;
  private static final int METHODID_SERVICE5 = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SERVICE1:
          serviceImpl.service1((Example.ServiceRequest) request,
              (io.grpc.stub.StreamObserver<Example.ServiceResponse>) responseObserver);
          break;
        case METHODID_SERVICE2:
          serviceImpl.service2((Example.ServiceRequest) request,
              (io.grpc.stub.StreamObserver<Example.ServiceResponse>) responseObserver);
          break;
        case METHODID_SERVICE3:
          serviceImpl.service3((Example.ServiceRequest) request,
              (io.grpc.stub.StreamObserver<Example.ServiceResponse>) responseObserver);
          break;
        case METHODID_SERVICE4:
          serviceImpl.service4((Example.ServiceRequest) request,
              (io.grpc.stub.StreamObserver<Example.ServiceResponse>) responseObserver);
          break;
        case METHODID_SERVICE5:
          serviceImpl.service5((Example.ServiceRequest) request,
              (io.grpc.stub.StreamObserver<Example.ServiceResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getService1Method(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              Example.ServiceRequest,
              Example.ServiceResponse>(
                service, METHODID_SERVICE1)))
        .addMethod(
          getService2Method(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              Example.ServiceRequest,
              Example.ServiceResponse>(
                service, METHODID_SERVICE2)))
        .addMethod(
          getService3Method(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              Example.ServiceRequest,
              Example.ServiceResponse>(
                service, METHODID_SERVICE3)))
        .addMethod(
          getService4Method(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              Example.ServiceRequest,
              Example.ServiceResponse>(
                service, METHODID_SERVICE4)))
        .addMethod(
          getService5Method(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              Example.ServiceRequest,
              Example.ServiceResponse>(
                service, METHODID_SERVICE5)))
        .build();
  }

  private static abstract class TestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TestServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return Example.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TestService");
    }
  }

  private static final class TestServiceFileDescriptorSupplier
      extends TestServiceBaseDescriptorSupplier {
    TestServiceFileDescriptorSupplier() {}
  }

  private static final class TestServiceMethodDescriptorSupplier
      extends TestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TestServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (TestServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TestServiceFileDescriptorSupplier())
              .addMethod(getService1Method())
              .addMethod(getService2Method())
              .addMethod(getService3Method())
              .addMethod(getService4Method())
              .addMethod(getService5Method())
              .build();
        }
      }
    }
    return result;
  }
}
