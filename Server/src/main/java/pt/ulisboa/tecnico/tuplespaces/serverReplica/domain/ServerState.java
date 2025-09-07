package pt.ulisboa.tecnico.tuplespaces.serverReplica.domain;

import java.nio.channels.CancelledKeyException;
import java.util.ArrayList;
import java.util.List;

import io.grpc.StatusRuntimeException;
import io.grpc.Status;

public class ServerState {

  private List<String> tuples;

  private final boolean DEBUG_FLAG;

  public ServerState(boolean DEBUG_FLAG) {
    this.tuples = new ArrayList<String>();
    this.DEBUG_FLAG = DEBUG_FLAG;
  }

  /** Helper method to print debug messages. */
  private void debug(String debugMessage) {
    if (this.DEBUG_FLAG)
      System.err.println(debugMessage);
  }

  /** Put operation */
  public synchronized String put(String tuple) {
    
    tuples.add(tuple);
    notifyAll(); // notifies that a tuple has been put into the TupleSpace
    return "OK"; // sends acknowledgement
  }

  /** Helper method to get the matching tuples to a given search pattern */
  private String getMatchingTuple(String pattern) {
    for (String tuple : this.tuples) {
      if (tuple.matches(pattern)) {
        return tuple;
      }
    }
    return null;
  }

  /** Read operation */
  public synchronized String read(String pattern) {

    String tuple = getMatchingTuple(pattern);

    // blocks until a tuple that matches the pattern is found
    while (tuple == null) {
      try {
        debug("Waiting for a matching tuple...");
        wait(); // wait until a tuple is put into the TupleSpace
      } catch (InterruptedException e) {
        System.err.println("Caught Interrupted Exception: " + e);
        throw Status.CANCELLED.withDescription("[ERROR] Interrupted").asRuntimeException();
      }
      tuple = getMatchingTuple(pattern);
    }
    
    return tuple;
  }

  /** Take operation */
  public synchronized String take(String pattern) {
    
    String tuple = getMatchingTuple(pattern);

    // blocks until a tuple that matches the pattern is found
    while (tuple == null) {
      try {
        debug("Waiting for a matching tuple...");
        wait(); // wait until a tuple is put into the TupleSpace
      } catch (InterruptedException e) {
        System.err.println("Caught Interrupted Exception: " + e);
        throw Status.CANCELLED.withDescription("[ERROR] Interrupted").asRuntimeException();
      }
      tuple = getMatchingTuple(pattern);
    }

    this.tuples.remove(tuple); // takes the tuple from the TupleSpace
    return tuple;
  }

  /** getTupleSpacesState operation */
  public synchronized List<String> getTupleSpacesState() {
    
    return this.tuples;
  }

  /** Gets all the matching tuples for a given a pattern */
  public List<String> getMatchingTuples(String pattern) {
    ArrayList<String> matchingTuples = new ArrayList<String>();

    for (String tuple : this.tuples) {
      if (tuple.matches(pattern)) {
        matchingTuples.add(tuple);
      }
    }
    return matchingTuples;
  }
}