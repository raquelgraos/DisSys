package pt.ulisboa.tecnico.tuplespaces.frontend;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.io.IOException;

public class FrontEndMain {

    /** Helper method to print debug messages. */
    private static void debug(boolean DEBUG_FLAG, String debugMessage) {
      if (DEBUG_FLAG)
        System.err.println(debugMessage);
    }

    public static void main(String[] args) {

      /** Set flag to true to print debug messages. 
      * The flag can be set adding "-debug" to frontend args */
      boolean DEBUG_FLAG = false;

      System.out.println(FrontEndMain.class.getSimpleName());

      // check arguments
      if (args.length < 4 || 
          (args.length == 4 && 
          (args[0].equals("-debug") || args[1].equals("-debug")) && args[2].equals("-debug") || args[3].equals("-debug"))) {
        System.err.println("Arguments missing!");
        System.err.printf("Usage: mvn exec:java -Dexec.args= <port> <host:port> <host:port2> <host:port3> (-debug)");
        return;
      }

      // turns on DEBUG mode
      if (args.length == 5 && args[4].equals("-debug")) {
        DEBUG_FLAG = true;
      }
      
      // receive and print arguments
      debug(DEBUG_FLAG, String.format("Received %d arguments", args.length));
      for (int i = 0; i < args.length; i++) {
        debug(DEBUG_FLAG, String.format("arg[%d] = %s", i, args[i]));
      }

      int numServers = 3;

      final int port = Integer.parseInt(args[0]);
      final String[] host_ports = new String[numServers];

      for (int i = 0; i < numServers; i++) {
        host_ports[i] = args[i+1];
      }

      final BindableService impl = new FrontEndService(host_ports, DEBUG_FLAG, numServers);

      // create a new server to listen on port, with an interceptor
      Server frontend = ServerBuilder.forPort(port).addService(ServerInterceptors.intercept(impl, new HeaderFrontendInterceptor())).build();

      try {
        // start the frontend
        frontend.start();

        // server threads are running in the background
        debug(DEBUG_FLAG, "Front-End started!");

        // do not exit the main thread. Wait until frontend is terminated
        frontend.awaitTermination();

        // shutdowns the channel
        ((FrontEndService) impl).shutdown();

      } catch (IOException e) {
        System.out.println("Caught IOException: " + e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        System.out.println("Caught InterruptedException: " + e);
        throw new RuntimeException(e);
      }

    }
    
}