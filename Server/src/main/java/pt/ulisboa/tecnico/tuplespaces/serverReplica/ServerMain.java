package pt.ulisboa.tecnico.tuplespaces.serverReplica;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import io.grpc.ServerInterceptors;

import pt.ulisboa.tecnico.tuplespaces.serverReplica.domain.*;


public class ServerMain {

    /** Helper method to print debug messages. */
    private static void debug(boolean DEBUG_FLAG, String debugMessage) {
      if (DEBUG_FLAG)
        System.err.println(debugMessage);
    }

    public static void main(String[] args) {

      /** Set flag to true to print debug messages. 
      * The flag can be set adding "-debug" to server args */
      boolean DEBUG_FLAG = false;

      System.out.println(ServerMain.class.getSimpleName());

      // check arguments
      if (args.length < 1 || (args.length == 1 && (args[0].equals("-debug")))) {
        System.err.println("Argument missing!");
        System.err.printf("Usage: mvn exec:java -Dexec.args = \"<port> (-debug)\"\n");
        return;
      }

      // turns on DEBUG mode
      if (args.length == 2 && (args[1].equals("-debug"))) {
        DEBUG_FLAG = true;
      }

      // receive and print arguments
      debug(DEBUG_FLAG, String.format("Received %d arguments", args.length));
      for (int i = 0; i < args.length; i++) {
        debug(DEBUG_FLAG, String.format("arg[%d] = %s", i, args[i]));
      }

      final int port = Integer.parseInt(args[0]);
      final BindableService impl = new TupleServiceImpl(DEBUG_FLAG);

      // create a new server to listen on port with interceptor
      Server server = ServerBuilder.forPort(port).addService(ServerInterceptors.intercept(impl, new HeaderServerInterceptor())).build();

      try {
        // start the server
        server.start();

        // server threads are running in the background
        debug(DEBUG_FLAG, "Server started!");

        // do not exit the main thread. Wait until server is terminated
        server.awaitTermination();

      } catch (IOException e) {
        System.out.println("Caught IOException: " + e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        System.out.println("Caught InterruptedException: " + e);
        throw new RuntimeException(e);

      }

    }
}

