package pt.ulisboa.tecnico.tuplespaces.centralized.contract;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.0)",
    comments = "Source: TupleSpaces2.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TupleSpaces2Grpc {

  private TupleSpaces2Grpc() {}

  public static final String SERVICE_NAME = "pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse> getPutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "put",
      requestType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest.class,
      responseType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse> getPutMethod() {
    io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse> getPutMethod;
    if ((getPutMethod = TupleSpaces2Grpc.getPutMethod) == null) {
      synchronized (TupleSpaces2Grpc.class) {
        if ((getPutMethod = TupleSpaces2Grpc.getPutMethod) == null) {
          TupleSpaces2Grpc.getPutMethod = getPutMethod =
              io.grpc.MethodDescriptor.<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "put"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TupleSpaces2MethodDescriptorSupplier("put"))
              .build();
        }
      }
    }
    return getPutMethod;
  }

  private static volatile io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "read",
      requestType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest.class,
      responseType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse> getReadMethod() {
    io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse> getReadMethod;
    if ((getReadMethod = TupleSpaces2Grpc.getReadMethod) == null) {
      synchronized (TupleSpaces2Grpc.class) {
        if ((getReadMethod = TupleSpaces2Grpc.getReadMethod) == null) {
          TupleSpaces2Grpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TupleSpaces2MethodDescriptorSupplier("read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse> getTakeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "take",
      requestType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest.class,
      responseType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse> getTakeMethod() {
    io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse> getTakeMethod;
    if ((getTakeMethod = TupleSpaces2Grpc.getTakeMethod) == null) {
      synchronized (TupleSpaces2Grpc.class) {
        if ((getTakeMethod = TupleSpaces2Grpc.getTakeMethod) == null) {
          TupleSpaces2Grpc.getTakeMethod = getTakeMethod =
              io.grpc.MethodDescriptor.<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "take"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TupleSpaces2MethodDescriptorSupplier("take"))
              .build();
        }
      }
    }
    return getTakeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse> getGetTupleSpacesStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "getTupleSpacesState",
      requestType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest.class,
      responseType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse> getGetTupleSpacesStateMethod() {
    io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse> getGetTupleSpacesStateMethod;
    if ((getGetTupleSpacesStateMethod = TupleSpaces2Grpc.getGetTupleSpacesStateMethod) == null) {
      synchronized (TupleSpaces2Grpc.class) {
        if ((getGetTupleSpacesStateMethod = TupleSpaces2Grpc.getGetTupleSpacesStateMethod) == null) {
          TupleSpaces2Grpc.getGetTupleSpacesStateMethod = getGetTupleSpacesStateMethod =
              io.grpc.MethodDescriptor.<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "getTupleSpacesState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TupleSpaces2MethodDescriptorSupplier("getTupleSpacesState"))
              .build();
        }
      }
    }
    return getGetTupleSpacesStateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse> getEnterMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "enter",
      requestType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest.class,
      responseType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse> getEnterMethod() {
    io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse> getEnterMethod;
    if ((getEnterMethod = TupleSpaces2Grpc.getEnterMethod) == null) {
      synchronized (TupleSpaces2Grpc.class) {
        if ((getEnterMethod = TupleSpaces2Grpc.getEnterMethod) == null) {
          TupleSpaces2Grpc.getEnterMethod = getEnterMethod =
              io.grpc.MethodDescriptor.<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "enter"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TupleSpaces2MethodDescriptorSupplier("enter"))
              .build();
        }
      }
    }
    return getEnterMethod;
  }

  private static volatile io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse> getUnlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "unlock",
      requestType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest.class,
      responseType = pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest,
      pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse> getUnlockMethod() {
    io.grpc.MethodDescriptor<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse> getUnlockMethod;
    if ((getUnlockMethod = TupleSpaces2Grpc.getUnlockMethod) == null) {
      synchronized (TupleSpaces2Grpc.class) {
        if ((getUnlockMethod = TupleSpaces2Grpc.getUnlockMethod) == null) {
          TupleSpaces2Grpc.getUnlockMethod = getUnlockMethod =
              io.grpc.MethodDescriptor.<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest, pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "unlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TupleSpaces2MethodDescriptorSupplier("unlock"))
              .build();
        }
      }
    }
    return getUnlockMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TupleSpaces2Stub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TupleSpaces2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TupleSpaces2Stub>() {
        @java.lang.Override
        public TupleSpaces2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TupleSpaces2Stub(channel, callOptions);
        }
      };
    return TupleSpaces2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TupleSpaces2BlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TupleSpaces2BlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TupleSpaces2BlockingStub>() {
        @java.lang.Override
        public TupleSpaces2BlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TupleSpaces2BlockingStub(channel, callOptions);
        }
      };
    return TupleSpaces2BlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TupleSpaces2FutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TupleSpaces2FutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TupleSpaces2FutureStub>() {
        @java.lang.Override
        public TupleSpaces2FutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TupleSpaces2FutureStub(channel, callOptions);
        }
      };
    return TupleSpaces2FutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class TupleSpaces2ImplBase implements io.grpc.BindableService {

    /**
     */
    public void put(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutMethod(), responseObserver);
    }

    /**
     */
    public void read(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    public void take(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTakeMethod(), responseObserver);
    }

    /**
     */
    public void getTupleSpacesState(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTupleSpacesStateMethod(), responseObserver);
    }

    /**
     */
    public void enter(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnterMethod(), responseObserver);
    }

    /**
     */
    public void unlock(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUnlockMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPutMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest,
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse>(
                  this, METHODID_PUT)))
          .addMethod(
            getReadMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest,
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse>(
                  this, METHODID_READ)))
          .addMethod(
            getTakeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest,
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse>(
                  this, METHODID_TAKE)))
          .addMethod(
            getGetTupleSpacesStateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest,
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse>(
                  this, METHODID_GET_TUPLE_SPACES_STATE)))
          .addMethod(
            getEnterMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest,
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse>(
                  this, METHODID_ENTER)))
          .addMethod(
            getUnlockMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest,
                pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse>(
                  this, METHODID_UNLOCK)))
          .build();
    }
  }

  /**
   */
  public static final class TupleSpaces2Stub extends io.grpc.stub.AbstractAsyncStub<TupleSpaces2Stub> {
    private TupleSpaces2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TupleSpaces2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TupleSpaces2Stub(channel, callOptions);
    }

    /**
     */
    public void put(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void read(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void take(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTakeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTupleSpacesState(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTupleSpacesStateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void enter(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnterMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unlock(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest request,
        io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUnlockMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class TupleSpaces2BlockingStub extends io.grpc.stub.AbstractBlockingStub<TupleSpaces2BlockingStub> {
    private TupleSpaces2BlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TupleSpaces2BlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TupleSpaces2BlockingStub(channel, callOptions);
    }

    /**
     */
    public pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse put(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutMethod(), getCallOptions(), request);
    }

    /**
     */
    public pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse read(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse take(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTakeMethod(), getCallOptions(), request);
    }

    /**
     */
    public pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse getTupleSpacesState(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTupleSpacesStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse enter(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnterMethod(), getCallOptions(), request);
    }

    /**
     */
    public pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse unlock(pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUnlockMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class TupleSpaces2FutureStub extends io.grpc.stub.AbstractFutureStub<TupleSpaces2FutureStub> {
    private TupleSpaces2FutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TupleSpaces2FutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TupleSpaces2FutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse> put(
        pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse> read(
        pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse> take(
        pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTakeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse> getTupleSpacesState(
        pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTupleSpacesStateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse> enter(
        pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnterMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse> unlock(
        pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUnlockMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PUT = 0;
  private static final int METHODID_READ = 1;
  private static final int METHODID_TAKE = 2;
  private static final int METHODID_GET_TUPLE_SPACES_STATE = 3;
  private static final int METHODID_ENTER = 4;
  private static final int METHODID_UNLOCK = 5;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TupleSpaces2ImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TupleSpaces2ImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUT:
          serviceImpl.put((pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest) request,
              (io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest) request,
              (io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse>) responseObserver);
          break;
        case METHODID_TAKE:
          serviceImpl.take((pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest) request,
              (io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse>) responseObserver);
          break;
        case METHODID_GET_TUPLE_SPACES_STATE:
          serviceImpl.getTupleSpacesState((pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest) request,
              (io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse>) responseObserver);
          break;
        case METHODID_ENTER:
          serviceImpl.enter((pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest) request,
              (io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse>) responseObserver);
          break;
        case METHODID_UNLOCK:
          serviceImpl.unlock((pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest) request,
              (io.grpc.stub.StreamObserver<pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse>) responseObserver);
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

  private static abstract class TupleSpaces2BaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TupleSpaces2BaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TupleSpaces2");
    }
  }

  private static final class TupleSpaces2FileDescriptorSupplier
      extends TupleSpaces2BaseDescriptorSupplier {
    TupleSpaces2FileDescriptorSupplier() {}
  }

  private static final class TupleSpaces2MethodDescriptorSupplier
      extends TupleSpaces2BaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TupleSpaces2MethodDescriptorSupplier(String methodName) {
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
      synchronized (TupleSpaces2Grpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TupleSpaces2FileDescriptorSupplier())
              .addMethod(getPutMethod())
              .addMethod(getReadMethod())
              .addMethod(getTakeMethod())
              .addMethod(getGetTupleSpacesStateMethod())
              .addMethod(getEnterMethod())
              .addMethod(getUnlockMethod())
              .build();
        }
      }
    }
    return result;
  }
}
