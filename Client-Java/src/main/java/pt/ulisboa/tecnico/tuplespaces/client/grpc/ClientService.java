package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse;

public class ClientService {

    private final ManagedChannel channel;
    private TupleSpacesGrpc.TupleSpacesBlockingStub stub;
    private TupleSpacesGrpc.TupleSpacesBlockingStub stubWithHeader;
    private final TupleSpacesGrpc.TupleSpacesBlockingStub stubWhithoutHeaders;
    private final int clientId;
    private final boolean DEBUG_FLAG;
    private Metadata delaysMetadata;
    private int order = 1;

    // key for sending replicated server delays as metadata to frontend
    static final Metadata.Key<String> DELAYS_HEADER_FROM_CLIENT_KEY =
        Metadata.Key.of("delays_header_from_client_key", Metadata.ASCII_STRING_MARSHALLER);


    /** Helper method to print debug messages. */
    private void debug(String debugMessage) {
        if (this.DEBUG_FLAG)
            System.err.println(debugMessage);
    }


    public ClientService(boolean DEBUG_FLAG, String host_port, int clientId) {

        this.DEBUG_FLAG = DEBUG_FLAG;

        // Assign clientId
        this.clientId = clientId;

        debug(String.format("Connecting to server at %s", host_port));

        // Create Channel
        this.channel = ManagedChannelBuilder.forTarget(host_port).usePlaintext().build();

        this.delaysMetadata = new Metadata();

        // Create blocking stub with no header to use when there are no delays to send
        this.stubWhithoutHeaders = TupleSpacesGrpc.newBlockingStub(channel); 

        this.stubWithHeader = this.stubWhithoutHeaders.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(this.delaysMetadata));

        this.stub = this.stubWhithoutHeaders;
    }

    public String put(String tuple, String delays) {
        debug("Calling put() with tuple: " + tuple + " - REQUEST #" + order);
        // Create a request with the tuple
        PutRequest request = PutRequest.newBuilder().setNewTuple(tuple).setClientId(this.clientId).setOrder(this.order).build();

        // Increments the client's order number
        this.order += 1;

        // if there are delays, sends them in a stubWithHeaders
        if (delays != null){
            this.delaysMetadata.put(DELAYS_HEADER_FROM_CLIENT_KEY, delays);
            this.stub = this.stubWithHeader;
        } else {
            this.stub = this.stubWhithoutHeaders;
        }

        // Call the put method on the stub
        try {
            this.stub.put(request);
            debug(String.format("Put operation successful"));
            return "OK";

        } catch (StatusRuntimeException e) {
            handleGrpcError(e);
        }
        return null;
    }

    public String read(String pattern, String delays) {
        debug("Calling read() with pattern: " + pattern + " - REQUEST #" + order);
        ReadRequest request = ReadRequest.newBuilder().setSearchPattern(pattern).setClientId(this.clientId).setOrder(this.order).build();

        // Increments the client's order number
        this.order += 1;

        // if there are delays, sends them in a stubWithHeaders
        if (delays != null){
            this.delaysMetadata.put(DELAYS_HEADER_FROM_CLIENT_KEY, delays);
            this.stub = this.stubWithHeader;
        } else {
            this.stub = this.stubWhithoutHeaders;
        }

        try {
            ReadResponse response = this.stub.read(request);

            if (response.getResult().equals("NOK")) {
                debug("Read operation received some inconsistent results.");
            } else {
                debug(String.format("Read operation successful, server returned: %s", response.getResult()));
            }
            return response.getResult();

        } catch (StatusRuntimeException e) {
            handleGrpcError(e);
        }
        return null;
    }

    public String take(String tuple, String delays) {
        debug("Calling take() with tuple: " + tuple + " - REQUEST #" + order);
        TakeRequest request = TakeRequest.newBuilder().setSearchPattern(tuple).setClientId(this.clientId).setOrder(this.order).build();

        // Increments the client's order number
        this.order += 1;

        // if there are delays, sends them in a stubWithHeaders
        if (delays != null){
            this.delaysMetadata.put(DELAYS_HEADER_FROM_CLIENT_KEY, delays);
            this.stub = this.stubWithHeader;
        } else {
            this.stub = this.stubWhithoutHeaders;
        }
        
        try {
            TakeResponse response = stub.take(request);
            debug(String.format("Take operation successful, server returned: %s", response.getResult()));
            return response.getResult();

        } catch (StatusRuntimeException e) {
            handleGrpcError(e);
        }
        return null;
    }

    public String getTupleSpacesState() {
        debug("Calling getTupleSpacesState() - REQUEST #" + order);
        getTupleSpacesStateRequest request = getTupleSpacesStateRequest.newBuilder().setClientId(this.clientId).setOrder(this.order).build();

        // Increments the client's order number
        this.order += 1;

        this.stub = this.stubWhithoutHeaders;

        try {
            getTupleSpacesStateResponse response = stub.getTupleSpacesState(request);
            debug(String.format("getTupleSpacesState operation successful, server returned: %s", response.getTupleList()));
            List<String> tuples = response.getTupleList();
            return tuples.isEmpty() ? "[]" : "[" + String.join(", ", tuples) + "]";

        } catch (StatusRuntimeException e) {
            handleGrpcError(e);
        }
        return null;
    }

    public void shutdown() {

        channel.shutdown();
    }

 
    // Handles gRPC errors and provides error messages.
    private void handleGrpcError(StatusRuntimeException e) {
        // Extract the error code and description from the gRPC exception.
        Status.Code code = e.getStatus().getCode();
        String description = e.getStatus().getDescription();

        switch (code) {
            case UNAVAILABLE:
                if ("[ERROR] Server is unavailable".equals(description)) {
                    System.err.println("[ERROR] The server is unavailable.");
                } else {
                    System.err.println("[ERROR] The front-end is unavailable.");
                }
                break;
            case CANCELLED:
                System.err.println("[ERROR] Request was interrupted");
            default:
                System.err.println("[ERROR] An unexpected error occurred: " + description);
        }
    }
}
