package pt.ulisboa.tecnico.tuplespaces.frontend;


import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;

public class HeaderFrontendInterceptor implements ServerInterceptor {

  // context for passing the delay values to the service
  public static final Context.Key<String> HEADER_VALUE_CONTEXT_KEY = Context.key("contextKey");

  // key for receiving metadata from client
  static final Metadata.Key<String> DELAYS_HEADER_FROM_CLIENT_KEY =
      Metadata.Key.of("delays_header_from_client_key", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      final Metadata requestHeaders,
      ServerCallHandler<ReqT, RespT> next) {
        
        String delayValues = requestHeaders.get(DELAYS_HEADER_FROM_CLIENT_KEY);

        if (delayValues != null) {
          // attach value to the gRPC Context as in metadata lab
          Context context = Context.current().withValue(HEADER_VALUE_CONTEXT_KEY, delayValues);
          return Contexts.interceptCall(context, call, requestHeaders, next);
        }


        return next.startCall(call, requestHeaders);
  }
} 
