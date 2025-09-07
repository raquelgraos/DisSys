package pt.ulisboa.tecnico.tuplespaces.frontend;

import java.io.IOError;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;

import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2Grpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.PutResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.ReadResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.TakeResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesOuterClass.getTupleSpacesStateResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.EnterResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockRequest;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.UnlockResponse;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FrontEndService extends TupleSpacesGrpc.TupleSpacesImplBase {

    private final ManagedChannel[] channels;
    private final TupleSpaces2Grpc.TupleSpaces2Stub[] stubs;
    private final TupleSpaces2Grpc.TupleSpaces2Stub[] stubsWithoutHeader;
    private final TupleSpaces2Grpc.TupleSpaces2Stub[] stubsWithHeader;
    private Metadata[] delayMetadatas;

    private final boolean DEBUG_FLAG;
    private final int numServers;

    // Key for sending metadata delays to servers
    static final Metadata.Key<String> DELAY_HEADER_TO_SERVER_KEY =
        Metadata.Key.of("delay_header_to_server_key", Metadata.ASCII_STRING_MARSHALLER);

    // Key for receiving metadata delays from clients
    static final Metadata.Key<String> DELAYS_HEADER_FROM_CLIENT_KEY =
        Metadata.Key.of("delays_header_from_client_key", Metadata.ASCII_STRING_MARSHALLER);

    private final ConcurrentHashMap<Integer, Boolean> takeCompletion = new ConcurrentHashMap<>();

    public FrontEndService(String[] host_ports, boolean DEBUG_FLAG, int numServers) {

        this.DEBUG_FLAG = DEBUG_FLAG;

        this.numServers = numServers;

        // Create channels and stubs
        this.channels = new ManagedChannel[numServers];
        this.stubs = new TupleSpaces2Grpc.TupleSpaces2Stub[numServers];
        
        //uses stub without headers when there are no delays
        this.stubsWithoutHeader = new TupleSpaces2Grpc.TupleSpaces2Stub[numServers];

        this.stubsWithHeader = new TupleSpaces2Grpc.TupleSpaces2Stub[numServers];

        this.delayMetadatas = new Metadata[numServers];

        for (int i = 0; i < numServers; i++) {
            this.delayMetadatas[i] = new Metadata();
            this.channels[i] = ManagedChannelBuilder.forTarget(host_ports[i]).usePlaintext().build();
            this.stubsWithoutHeader[i] = TupleSpaces2Grpc.newStub(channels[i]);
            this.stubsWithHeader[i] = this.stubsWithoutHeader[i].withInterceptors(MetadataUtils.newAttachHeadersInterceptor(this.delayMetadatas[i]));
            this.stubs[i] = this.stubsWithoutHeader[i];
        }
    }

    /** Helper method to print debug messages. */
    private void debug(String debugMessage) {
      if (this.DEBUG_FLAG)
        System.err.println(debugMessage);
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {

        debug("Redirecting put()...");
        
        // Gets the delay values from the metadata sent from client
        String delayValuesToParse = HeaderFrontendInterceptor.HEADER_VALUE_CONTEXT_KEY.get();
        
        // If no delays are provided, use the original stubs without any metadata
        for (int i = 0; i < this.numServers; i++){
            this.stubs[i] = this.stubsWithoutHeader[i];
        }

        if (delayValuesToParse != null){
            debug("delayValuesToParse: " + delayValuesToParse);

            String delays[] = delayValuesToParse.split(" ");

            for (int i = 0; i < delays.length; i++){
                // If delays are provided, update metadata and use the stubs with header
                delayMetadatas[i].put(DELAY_HEADER_TO_SERVER_KEY, delays[i]);
                this.stubs[i] = this.stubsWithHeader[i];
            }
        }

        int clientId = request.getClientId();

        if (takeCompletion.containsKey(clientId)) {
            // Ensures all take requests were completed before moving on to the put request
            synchronized (this) {
                while (!takeCompletion.get(clientId)) {
                    try {
                        debug("Waiting for previous Take Request to complete");
                        wait();
                    } catch (InterruptedException e) {
                        System.err.println("[Error] Request was interrupted");
                    }
                }
            }
        }

        // Builds a final response
        PutResponse response = PutResponse.newBuilder().setAck("OK").build();

        try {

            // Creates the countdownLatch to keep count of how many acknowledgments have been received
            CountDownLatch countDownLatch = new CountDownLatch(this.numServers);

            // Forwards the put request to the TupleSpaces servers
            for (int i = 0; i < this.numServers; i++) {
                int serverIndex = i;
                this.stubs[i].put(request, new StreamObserver<PutResponse>() {

                    @Override
                    public void onNext(PutResponse r) {
                        debug("Received PutResponse: " + r.getAck() + " from Replica " + serverIndex);
                        countDownLatch.countDown();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Status status = Status.fromThrowable(throwable);

                        switch(status.getCode()) {
                            case UNAVAILABLE:
                                System.err.println("[ERROR] Server is unavailable");
                                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("[ERROR] Server is unavailable")));
                                break;
                            case CANCELLED:
                                System.err.println("[Error] Request was interrupted");
                                responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                                break;
                            default:
                                System.err.println("[ERROR] An unexpected error occurred:" + status.getDescription());
                                responseObserver.onError(new StatusRuntimeException(status));
                                break;
                        }
                    }

                    @Override
                    public void onCompleted() {
                        debug("PutRequest completed");
                    }
                }); 
                if (i == this.numServers - 1){
                    // Forwards the server's response to the client
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }             
            }

            // Waits for responses from all replicas
            while (countDownLatch.getCount() > 0){
                countDownLatch.await();
            }
            
        } catch (IllegalStateException e) {
            System.err.println("[ERROR] onError was already sent");
        } catch (InterruptedException e) {
            System.err.println("[Error] Request was interrupted");
        }
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {

        debug("Redirecting read()...");

        // Create the responseCollector
        ResponseCollector collector = new ResponseCollector();

        // Tool to prevent multiple responses being added to the collector
        AtomicBoolean firstResponse = new AtomicBoolean(true);

        // Gets the delay values from the metadata sent from client
        String delayValuesToParse = HeaderFrontendInterceptor.HEADER_VALUE_CONTEXT_KEY.get();

        // If no delays are provided, use the original stubs without any metadata
        for (int i = 0; i < this.numServers; i++){
            this.stubs[i] = this.stubsWithoutHeader[i];
        }

        if (delayValuesToParse != null){
            debug("delayValuesToParse: " + delayValuesToParse);

            String delays[] = delayValuesToParse.split(" ");

            for (int i = 0; i < delays.length; i++){
                // If delays are provided, update metadata and use the stubs with header
                delayMetadatas[i].put(DELAY_HEADER_TO_SERVER_KEY, delays[i]);
                this.stubs[i] = this.stubsWithHeader[i];
            }
        }

        AtomicInteger errorCounter = new AtomicInteger(0);

        try {

            // Creates the countdownLatch to keep count of the received responses
            CountDownLatch countDownLatch = new CountDownLatch(1);

            // Forwards the request to the TupleSpaces servers
            for (int i = 0; i < this.numServers; i++) {
                int numServers = this.numServers;
                this.stubs[i].read(request, new StreamObserver<ReadResponse>() {

                    @Override
                    public void onNext(ReadResponse r) {
                        // Ensures that only the first response is considered
                        if (firstResponse.compareAndSet(true, false)) {
                            collector.addResponse(r.getResult());
                            countDownLatch.countDown();
                            debug("Received ReadResponse: " + r.getResult());
                        }
                    }   

                    @Override
                    public void onError(Throwable throwable) {
                        Status status = Status.fromThrowable(throwable);

                        switch(status.getCode()) {
                            case UNAVAILABLE:
                                System.err.println("[ERROR] Server is unavailable");
                                // If all servers are unavailable
                                if (errorCounter.incrementAndGet() >= numServers) {
                                    responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("[ERROR] Server is unavailable")));
                                }
                                break;
                            case CANCELLED:
                                System.err.println("[Error] Request was interrupted");
                                if (errorCounter.incrementAndGet() >= numServers) {
                                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                                }
                                break;
                            default:
                                System.err.println("[ERROR] An unexpected error occurred:" + status.getDescription());
                                if (errorCounter.incrementAndGet() >= numServers) {
                                    responseObserver.onError(new StatusRuntimeException(status));
                                }
                                break;
                        }
                    }

                    @Override
                    public void onCompleted() {
                        debug("ReadRequest completed");
                    }
                });
            }

            // Waits for a response from one of the replica servers
            while (countDownLatch.getCount() > 0){
                countDownLatch.await();
            }                

            String result = collector.getResponses();

            ReadResponse response = ReadResponse.newBuilder().setResult(result).build();

            // Forwards the server's response to the client
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IllegalStateException e) {
            System.err.println("[ERROR] onError was already sent");
        } catch (InterruptedException e) {
            System.err.println("[Error] Request was interrupted");
        }
    }

    @Override
    public void take(TakeRequest request, StreamObserver<TakeResponse> responseObserver) {

        String tuple = request.getSearchPattern();
        int clientId = request.getClientId();
        int order = request.getOrder();

        List<Integer> voterSet = new ArrayList<Integer>();
        voterSet.add(clientId % this.numServers);
        voterSet.add((clientId + 1) % this.numServers);

        // Marks this client as having a non completed take in course
        takeCompletion.put(clientId, false);

        // Creates a CountDownLatch to keep track of the voter set's responses
        CountDownLatch maekawaRequestCDLatch = new CountDownLatch(voterSet.size());

        // Resets the stubs to use the original stubs without any metadata
        for (int i = 0; i < this.numServers; i++){
            this.stubs[i] = this.stubsWithoutHeader[i];
        }

        // gets the delay values from the metadata sent from client
        String delayValuesToParse = HeaderFrontendInterceptor.HEADER_VALUE_CONTEXT_KEY.get();

        if (delayValuesToParse != null){
            debug("delayValuesToParse: " + delayValuesToParse);

            String delays[] = delayValuesToParse.split(" ");

            for (int i = 0; i < delays.length; i++){
                // If delays are provided, update metadata and use the stubs with header
                delayMetadatas[i].put(DELAY_HEADER_TO_SERVER_KEY, delays[i]);
                this.stubs[i] = this.stubsWithHeader[i];
            }
        }

        try {

            ResponseCollector matchesCollector = new ResponseCollector();
            
            // Sends an enterRequest to each process of the voter set
            EnterRequest enterRequest = EnterRequest.newBuilder().setTuple(tuple).setClientId(clientId).setOrder(order).build();

            for (Integer i : voterSet) {

                stubs[i].enter(enterRequest, new StreamObserver<EnterResponse>(){

                    @Override
                    public void onNext(EnterResponse r) {
                        matchesCollector.addMatchesList(r.getMatchesList());
                        maekawaRequestCDLatch.countDown();
                        debug("Got approval from server " + i + " of the Voter Set");
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        
                    }

                    @Override
                    public void onCompleted() {
                        debug("EnterRequest completed");
                    }
                });
            }

            // Waits for the responses of the voter set
            // Can not perform the take operation before all the voters approve 
            while (maekawaRequestCDLatch.getCount() > 0){
                maekawaRequestCDLatch.await();
            }

            // Checks if the replicas sent a common tuple (valid to take)
            String tupleToTake = matchesCollector.getTupleToTake();

            // Resets the stubs to use the original stubs without any metadataset
            for (int i = 0; i < this.numServers; i++){
                this.stubs[i] = this.stubsWithoutHeader[i];
            }

            int resentNumber = 1;
            
            // While there isn't a specific tuple to take, the locked tuples will be unlocked
            // and enterRequests will be resent
            while (tupleToTake == null) {

                CountDownLatch unlockRequestCDLatch = new CountDownLatch(voterSet.size());

                // Sends an unlockRequest to each process of the voter set
                UnlockRequest unlockRequest = UnlockRequest.newBuilder().setTuple(tuple).build();

                for (Integer i : voterSet) {

                    stubs[i].unlock(unlockRequest, new StreamObserver<UnlockResponse>(){

                        @Override
                        public void onNext(UnlockResponse r) {
                            unlockRequestCDLatch.countDown();
                            debug("Unlocked the tuple " + tuple);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            
                        }

                        @Override
                        public void onCompleted() {
                            debug("UnlockRequest completed");
                        }
                    });
                }

                while (unlockRequestCDLatch.getCount() > 0){
                    unlockRequestCDLatch.await();
                }

                CountDownLatch maekawaReRequestCDLatch = new CountDownLatch(voterSet.size());

                // Waits before resending the enterRequests
                Thread.sleep(resentNumber * 5000);

                ResponseCollector matchesReCollector = new ResponseCollector();
            
                // Sends an enterRequest to each process of the voter set
                enterRequest = EnterRequest.newBuilder().setTuple(tuple).setClientId(clientId).setOrder(order).build();

                for (Integer i : voterSet) {

                    stubs[i].enter(enterRequest, new StreamObserver<EnterResponse>(){

                        @Override
                        public void onNext(EnterResponse r) {
                            matchesReCollector.addMatchesList(r.getMatchesList());
                            maekawaReRequestCDLatch.countDown();
                            debug("Got approval from server " + i + " of the Voter Set");
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Status status = Status.fromThrowable(throwable);

                            switch(status.getCode()) {
                                case UNAVAILABLE:
                                    System.err.println("[ERROR] Server is unavailable");
                                    responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("[ERROR] Server is unavailable")));
                                    break;
                                case CANCELLED:
                                    System.err.println("[Error] Request was interrupted");
                                    responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                                    break;
                                default:
                                    System.err.println("[ERROR] An unexpected error occurred:" + status.getDescription());
                                    responseObserver.onError(new StatusRuntimeException(status));
                                    break;
                            }
                        }

                        @Override
                        public void onCompleted() {
                            debug("EnterRequest completed");
                        }
                    });
                }

                // Waits for the responses of the voter set
                // Can not perform the take operation before all the voters approve 
                while (maekawaReRequestCDLatch.getCount() > 0){
                    maekawaReRequestCDLatch.await();
                }

                // Checks if the replicas sent a common tuple (valid to take)
                tupleToTake = matchesReCollector.getTupleToTake();

                resentNumber += 1;
            }

            // Builds a new request with the chosen tuple to take
            request = TakeRequest.newBuilder().setSearchPattern(tupleToTake).setClientId(clientId).setOrder(order).setTuple(tuple).build();

            // Immediately responds to the client with the tuple to take
            TakeResponse response = TakeResponse.newBuilder().setResult(tupleToTake).build();

            // Forwards the server's response to the client
            responseObserver.onNext(response);
            responseObserver.onCompleted(); 

            debug("Entering critical section...");

            ResponseCollector collector = new ResponseCollector();
            
            debug("Redirecting take()...");

            // Creates the countdownLatch to keep count of
            // the received responses to the take request
            CountDownLatch countDownLatch = new CountDownLatch(this.numServers);

            // Forwards the take request to the TupleSpaces servers
            for (int i = 0; i < this.numServers; i++) {
                int k = i;
                stubs[i].take(request, new StreamObserver<TakeResponse>() {

                    @Override
                    public void onNext(TakeResponse r) {
                        collector.addResponse(r.getResult());
                        String responseAck = r.getAck();
                        if (responseAck.equals("RELEASED")) {
                            debug("Server replica " + k + " released successfully");
                        } else if (responseAck.equals("QUEUE")){
                            debug("Server replica " + k + " voted for another EnterRequest");
                        }
                        countDownLatch.countDown();
                        debug("Received TakeResponse: " + r.getResult());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Status status = Status.fromThrowable(throwable);

                        switch(status.getCode()) {
                            case UNAVAILABLE:
                                System.err.println("[ERROR] Server is unavailable");
                                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("[ERROR] Server is unavailable")));
                                break;
                            case CANCELLED:
                                System.err.println("[Error] Request was interrupted");
                                responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                                break;
                            default:
                                System.err.println("[ERROR] An unexpected error occurred:" + status.getDescription());
                                responseObserver.onError(new StatusRuntimeException(status));
                                break;
                        }
                    }

                    @Override
                    public void onCompleted() {
                        debug("TakeRequest completed");
                    }
                });
            }

            // Waits for a response from all of the replica servers
            while (countDownLatch.getCount() > 0){
                countDownLatch.await();
            }             

            String result = collector.compareAndGetResponse();

            debug("Exiting the critical section...");

            // Informs that the client has completed its Take Requests
            takeCompletion.replace(clientId, true);
            synchronized (this) {
                notify();
            }
            debug("Client " + clientId + " take status: " + takeCompletion.get(clientId));
            

        } catch (IllegalStateException e) {
            System.err.println("[ERROR] onError was already sent");
        } catch (InterruptedException e) {
            System.err.println("[Error] Request was interrupted");
        }
    }


    @Override
    public void getTupleSpacesState(getTupleSpacesStateRequest request, StreamObserver<getTupleSpacesStateResponse> responseObserver) {

        debug("Redirecting getTupleSpacesState()...");

        CountDownLatch countDownLatch = new CountDownLatch(this.numServers);

        ResponseCollector collector = new ResponseCollector();

        // Resets the stubs to use the original stubs without any metadata
        for (int i = 0; i < this.numServers; i++){
            this.stubs[i] = this.stubsWithoutHeader[i];
        }

        try {
            for (int i = 0; i < this.numServers; i++) {
                this.stubs[i].getTupleSpacesState(request, new StreamObserver<getTupleSpacesStateResponse>() {

                    @Override
                    public void onNext(getTupleSpacesStateResponse r) {
                        collector.addListResponse(r.getTupleList());
                        countDownLatch.countDown();
                        debug("Received response: " + r.getTupleList());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Status status = Status.fromThrowable(throwable);

                        switch(status.getCode()) {
                            case UNAVAILABLE:
                                System.err.println("[ERROR] Server is unavailable");
                                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("[ERROR] Server is unavailable")));
                                break;
                            case CANCELLED:
                                System.err.println("[Error] Request was interrupted");
                                responseObserver.onError(new StatusRuntimeException(Status.CANCELLED.withDescription("[Error] Request was interrupted")));
                            default:
                                System.err.println("[ERROR] An unexpected error occurred:" + status.getDescription());
                                responseObserver.onError(new StatusRuntimeException(status));
                                break;
                        }
                    }

                    @Override
                    public void onCompleted() {
                        debug("getTupleSpacesStateRequest completed");
                    }
                });

            }

            // Waits for all servers to reply
            while (countDownLatch.getCount() > 0){
                countDownLatch.await();
            }   

            List<String> result = collector.getResponsesList();

            getTupleSpacesStateResponse response = getTupleSpacesStateResponse.newBuilder().addAllTuple(result).build();

            // Forwards the server's response to the client
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IllegalStateException e) {
            System.err.println("[ERROR] onError was already sent");
        } catch (InterruptedException e) {
            System.err.println("[Error] Request was interrupted");
        }
    }

    public void shutdown() {
        for (int i = 0; i < numServers; i++)
            channels[i].shutdown();
    }
}