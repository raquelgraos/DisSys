package pt.ulisboa.tecnico.tuplespaces.serverReplica;

import java.io.ObjectInputFilter;

import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.serverReplica.domain.ServerState;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2Grpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.*;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

public class TupleServiceImpl extends TupleSpaces2Grpc.TupleSpaces2ImplBase {

    // Tuple Space Implementation
    private ServerState tupleSpace;

    private final boolean DEBUG_FLAG;

    // Stores the state and request queue for each tuple
    private final ConcurrentHashMap<String, Boolean> tupleLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<TupleRequestPair>> queue = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, Integer> clientsRequests = new ConcurrentHashMap<>();

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

        int clientId = request.getClientId();
        int order = request.getOrder();

        synchronized (this) {
            int last = clientsRequests.getOrDefault(clientId, 0);
            debug("[CLIENT "+ clientId + "] - PUT - Last request number: " + last);

            while (order > last + 1) {
                try {
                    debug("Waiting for missing requests from client " + clientId);
                    wait();
                    last = clientsRequests.getOrDefault(clientId, 0);
                } catch (InterruptedException e) {
                    System.err.println("[Error] Request was interrupted");
                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                    return;
                }
            }
        }

        try {
            String ack = tupleSpace.put(newTuple);

            PutResponse response = PutResponse.newBuilder().setAck(ack).build();
            
            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the put operation has been completed.
            responseObserver.onCompleted();

            synchronized (this) {
                debug("[CLIENT " + clientId + "] - PUT - NEW Last request: " + order);
                clientsRequests.put(clientId, order);
                notify();
            }

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

        int clientId = request.getClientId();
        int order = request.getOrder();

        synchronized (this) {
            int last = clientsRequests.getOrDefault(clientId, 0);
            debug("[CLIENT "+ clientId + "] - READ - Last request number: " + last);

            while (order > last + 1) {
                try {
                    debug("Waiting for missing requests from client " + clientId);
                    wait();
                    last = clientsRequests.getOrDefault(clientId, 0);
                } catch (InterruptedException e) {
                    System.err.println("[Error] Request was interrupted");
                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                    return;
                }
            }
        }

        try {
            String result = tupleSpace.read(searchPattern);

            ReadResponse response = ReadResponse.newBuilder().setResult(result).build();

            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the read operation has been completed.
            responseObserver.onCompleted();

            synchronized (this) {
                debug("[CLIENT " + clientId + "] - READ - NEW Last request: " + order);
                clientsRequests.put(clientId, order);
                notify();
            }

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
        String searchPattern = request.getSearchPattern(); // specific tuple to take
        String tuple = request.getTuple(); // original tuple

        debug(String.format("Calling take() with pattern: %s", searchPattern));

        int clientId = request.getClientId();
        int order = request.getOrder();

        synchronized (this) {
            int last = clientsRequests.getOrDefault(clientId, 0);
            debug("[CLIENT "+ clientId + "] - TAKE - Last request number: " + last);

            while (order > last + 1) {
                try {
                    debug("Waiting for missing requests from client " + clientId);
                    wait();
                    last = clientsRequests.getOrDefault(clientId, 0);
                } catch (InterruptedException e) {
                    System.err.println("[Error] Request was interrupted");
                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                    return;
                }
            }
        }

        try {
            String result = tupleSpace.take(searchPattern);

            TakeResponse response;

            // After succesful take, unlock the original tuple
            tupleLocks.replace(tuple, false);
            // no queue(when it isnt part of the voter set) or empty
            if (queue.get(tuple) == null || queue.get(tuple).isEmpty()) {
                // Responds to the frontend indicating that the tuple is unlocked (useful for debug)
                response = TakeResponse.newBuilder().setResult(result).setAck("RELEASED").build();
            } else {
                // releases Queue in order so that each request is either executed or gets in a queue for an earlier request
                releaseQueue(tuple);
                // Responds to the frontend indicating that queue was not empty 
                response = TakeResponse.newBuilder().setResult(result).setAck("QUEUE").build();
            }


            // Send a single response through the stream.
            responseObserver.onNext(response);
            // Notify the client that the take operation has been completed.
            responseObserver.onCompleted();

            synchronized (this) {
                debug("[CLIENT " + clientId + "] - TAKE - NEW Last request: " + order);
                clientsRequests.put(clientId, order);
                notify();
            }

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

    public void releaseQueue(String tuple){
        Boolean canExecuteTake;
        List<String> matches;
        EnterResponse nextResponse;
        StreamObserver<EnterResponse> nextRequest;
        for (int i = 0; i < queue.get(tuple).size(); i++){
            canExecuteTake = true;
            TupleRequestPair pair = queue.get(tuple).remove(0);
            // Checks if it needs to get in the queue of another tuple
            for (Map.Entry<String, Boolean> entry : tupleLocks.entrySet()) {
                String k = entry.getKey();
                Boolean v = entry.getValue();
                // Doesnt get in the queue of a request that is in its queue, 
                // and doesnt get in the queue of a request that is in the original queue but has lower priority
                if (v && pair.getTuple().equals(k) 
                        && !queue.get(pair.getTuple()).stream().anyMatch(p -> p.getTuple().equals(k))
                        && !queue.get(tuple).stream().anyMatch(p -> p.getTuple().equals(k))) {
                    debug("Tuple: " + tuple + "; added to: " + k + " queue");
                    queue.get(k).add(pair);
                    canExecuteTake = false;
                    break;
                }
            }
            // if it doesnt get in another queue, it can be executed
            if (canExecuteTake){
                nextRequest = pair.getRequest();
                matches = tupleSpace.getMatchingTuples(pair.getTuple());
                nextResponse = EnterResponse.newBuilder().addAllMatches(matches).build();
                nextRequest.onNext(nextResponse);
                nextRequest.onCompleted();
            }
        }
    }

    @Override
    public void getTupleSpacesState(getTupleSpacesStateRequest request, StreamObserver<getTupleSpacesStateResponse> responseObserver) {

        List<String> tuple = tupleSpace.getTupleSpacesState();

        debug("Calling getTupleSpacesState()");

        int clientId = request.getClientId();
        int order = request.getOrder();

        synchronized (this) {
            int last = clientsRequests.getOrDefault(clientId, 0);
            debug("[CLIENT "+ clientId + "] - GET - Last request number: " + last);
    
            while (order > last + 1) {
                try {
                    debug("Waiting for missing requests from client " + clientId);
                    wait();
                    last = clientsRequests.getOrDefault(clientId, 0);
                } catch (InterruptedException e) {
                    System.err.println("[Error] Request was interrupted");
                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                    return;
                }
            }
        }

        try {
            getTupleSpacesStateResponse response = getTupleSpacesStateResponse.newBuilder().addAllTuple(tuple).build();

            // Send a single response through the stream.
            responseObserver.onNext(response);

            // Notify the client that the getTupleSpacesState operation has been completed.
            responseObserver.onCompleted();

            synchronized (this) {
                debug("[CLIENT " + clientId + "] GET - NEW Last request: " + order);
                clientsRequests.put(clientId, order);
                notify();
            }

        } catch (StatusRuntimeException e) {
            // Sends error to frontend
            System.err.println("[ERROR] An unexpected error occurred:" + e.getStatus().getDescription());
            responseObserver.onError(e);

        }
    }

    @Override 
    public void enter(EnterRequest request, StreamObserver<EnterResponse> responseObserver) {
        String tuple = request.getTuple();
        debug("Calling enter() for tuple: " + tuple);

        int clientId = request.getClientId();
        int order = request.getOrder();

        synchronized (this) {
            int last = clientsRequests.getOrDefault(clientId, 0);
            debug("[CLIENT "+ clientId + "] - ENTER - Last request number: " + last);

            while (order > last + 1) {
                try {
                    debug("Waiting for missing requests from client " + clientId);
                    wait();
                    last = clientsRequests.getOrDefault(clientId, 0);
                } catch (InterruptedException e) {
                    System.err.println("[Error] Request was interrupted");
                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                    return;
                }
            }
        }

        List<String> matches = tupleSpace.getMatchingTuples(tuple);

        // Adds the called tuple to the tupleLocks & queue HashMaps if it wasn't there yet
        tupleLocks.putIfAbsent(tuple, false);
        queue.putIfAbsent(tuple, new ArrayList<>());

        // Verifies if there is already a tuple locked that forbids the new tuple
        // from moving to the critical section
        for (Map.Entry<String, Boolean> entry : tupleLocks.entrySet()) {
            String k = entry.getKey();
            Boolean v = entry.getValue();
        
            if (v && tuple.matches(k)) { 
                debug(" Cannot do take for " + tuple + " because " + k + " is locked and matches " + tuple);
                queue.get(k).add(new TupleRequestPair(tuple, responseObserver));
                tupleLocks.put(tuple, true);
                break;
            }
        }

        if (!tupleLocks.get(tuple)) {
            tupleLocks.put(tuple, true);
            EnterResponse response = EnterResponse.newBuilder().addAllMatches(matches).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override 
    public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
        String tuple = request.getTuple(); // Identify which tuple to unlock
        debug("Calling unlock() for tuple: " + tuple);
   
        // The tuple is unlocked
        tupleLocks.replace(tuple, false);
        UnlockResponse response = UnlockResponse.newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}