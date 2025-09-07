package pt.ulisboa.tecnico.tuplespaces.serverReplica;


import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;


public class HeaderServerInterceptor implements ServerInterceptor {

  // key for receiving metadata value from front-end
  static final Metadata.Key<String> DELAY_HEADER_TO_SERVER_KEY =
    Metadata.Key.of("delay_header_to_server_key", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      final Metadata requestHeaders,
      ServerCallHandler<ReqT, RespT> next) {
        
        String headerValue = requestHeaders.get(DELAY_HEADER_TO_SERVER_KEY);

        if (headerValue != null) {
          Integer time;
          
          try {
            time = Integer.parseInt(headerValue);
          } catch (NumberFormatException e) {
            System.err.println("Invalid delay header: " + headerValue);
            return next.startCall(call, requestHeaders);
          }
    
          try {
            Thread.sleep(time*1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        return next.startCall(call, requestHeaders);
        
  }
} 
    

