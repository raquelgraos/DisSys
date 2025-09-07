package pt.ulisboa.tecnico.tuplespaces.client;

import pt.ulisboa.tecnico.tuplespaces.client.grpc.ClientService;

public class ClientMain {

    /** Helper method to print debug messages. */
    private static void debug(boolean DEBUG_FLAG, String debugMessage) {
      if (DEBUG_FLAG)
        System.err.println(debugMessage);
    }

    public static void main(String[] args) {

        /** Set flag to true to print debug messages. 
        * The flag can be set adding "-debug" to server args */
        boolean DEBUG_FLAG = false;

        // check arguments
        if (args.length < 2 || (args.length == 2 && (args[0].equals("-debug") || args[1].equals("-debug")))) {
            System.err.println("Argument(s) missing!");
            System.err.println("Usage: mvn exec:java -Dexec.args=<host:port> <client_id>");
            return;
        }

        // turns on DEBUG mode
        if (args.length == 3 && (args[2].equals("-debug"))) {
            DEBUG_FLAG = true;
        }

        System.out.println(ClientMain.class.getSimpleName());

        // receive and print arguments
        debug(DEBUG_FLAG, String.format("Received %d arguments", args.length));
        for (int i = 0; i < args.length; i++) {
            debug(DEBUG_FLAG, String.format("arg[%d] = %s", i, args[i]));
        }

        // get the host and the port of the server or front-end
        final String host_port = args[0];
        final int clientId = Integer.parseInt(args[1]);

        CommandProcessor parser = new CommandProcessor(new ClientService(DEBUG_FLAG, host_port, clientId));
        parser.parseInput();

    }
}
