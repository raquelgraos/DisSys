package pt.ulisboa.tecnico.tuplespaces.serverReplica;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpaces2OuterClass.*;

public class TupleRequestPair {
    
    private final String tuple;
    private final StreamObserver<EnterResponse> request;

    public TupleRequestPair(String tuple, StreamObserver<EnterResponse> request){
        this.tuple = tuple;
        this.request = request;
    }

    public String getTuple(){
        return this.tuple;
    }

    public StreamObserver<EnterResponse>  getRequest(){
        return this.request;
    }

}
