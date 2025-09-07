package pt.ulisboa.tecnico.tuplespaces.server;

import java.io.ObjectInputFilter;

import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.server.domain.ServerState;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.*;

import java.util.List;

public class TupleServiceImpl extends TupleSpacesGrpc.TupleSpacesImplBase {

    // Tuple Space Implementation
    private ServerState tupleSpace;

    private final boolean DEBUG_FLAG;

    public TupleServiceImpl(boolean DEBUG_FLAG) {
        this.tupleSpace =  new ServerState(DEBUG_FLAG);
        this.DEBUG_FLAG = DEBUG_FLAG;
    }

    /** Helper method to print debug messages. */
    private void debug(String debugMessage) {
      if (this.DEBUG_FLAG)
        System.err.println(debugMessage);
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String newTuple = request.getNewTuple();

        debug(String.format("Calling put() with tuple: %s", newTuple));

        try {
            tupleSpace.put(newTuple);

            PutResponse response = PutResponse.newBuilder().build();
            
            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the put operation has been completed.
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            // Sends error to frontend
            System.err.println("[ERROR] An unexpected error occurred:" + e.getStatus().getDescription());
            responseObserver.onError(e);
        }
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        debug(String.format("Calling read() with pattern: %s", searchPattern));

        try {
            String result = tupleSpace.read(searchPattern);

            ReadResponse response = ReadResponse.newBuilder().setResult(result).build();

            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the read operation has been completed.
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.Code.CANCELLED) {
                // Sends interruption error to frontend
                System.err.println("[ERROR] Request was interrupted");
                responseObserver.onError(e);
            } else {
                // Sends error to frontend
                System.err.println("[ERROR] An unexpected error occurred:" + e.getStatus().getDescription());
                responseObserver.onError(e);
            }
        }

    }

    @Override
    public void take(TakeRequest request, StreamObserver<TakeResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        debug(String.format("Calling take() with pattern: %s", searchPattern));

        try {
            String result = tupleSpace.take(searchPattern);

            TakeResponse response = TakeResponse.newBuilder().setResult(result).build();

            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the take operation has been completed.
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.Code.CANCELLED) {
                // Sends interruption error to frontend
                System.err.println("[ERROR] Request was interrupted");
                responseObserver.onError(e);
            } else {
                // Sends error to frontend
                System.err.println("[ERROR] An unexpected error occurred:" + e.getStatus().getDescription());
                responseObserver.onError(e);
            }
        }

    }

    @Override
    public void getTupleSpacesState(getTupleSpacesStateRequest request, StreamObserver<getTupleSpacesStateResponse> responseObserver) {

        List<String> tuple = tupleSpace.getTupleSpacesState();

        debug("Calling getTupleSpacesState()");

        try {
            getTupleSpacesStateResponse response = getTupleSpacesStateResponse.newBuilder().addAllTuple(tuple).build();

            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the getTupleSpacesState operation has been completed.
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            // Sends error to frontend
            System.err.println("[ERROR] An unexpected error occurred:" + e.getStatus().getDescription());
            responseObserver.onError(e);

        }

    }
}