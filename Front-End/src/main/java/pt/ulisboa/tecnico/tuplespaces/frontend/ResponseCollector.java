package pt.ulisboa.tecnico.tuplespaces.frontend;

import java.util.ArrayList;
import java.util.List; 
import java.util.Set;
import java.util.HashSet;

public class ResponseCollector {

    private ArrayList<String> responses;
    private ArrayList<List<String>> matchingTuples;

    public ResponseCollector() {
        this.responses = new ArrayList<String>();
        this.matchingTuples = new ArrayList<>();
    }

    public synchronized void addResponse(String response) {
        this.responses.add(response);
    }

    public synchronized void addListResponse(List<String> response) {
        for (String s : response) {
            this.responses.add(s);
        }
    }

    public synchronized void addMatchesList(List<String> tuples) {
        this.matchingTuples.add(tuples);
    }

    public String getResponses() {
        String result = "";

        for (String s : responses) {
            result = result.concat(s);
        }
        return result;
    }

    // Ensures the responses are the same (as predicted) and returns it
    public String compareAndGetResponse() {

        String result = responses.get(0);

        for (String s : responses) {
            if (!s.equals(result)) return "NOK";
        }

        return result;
    }

    public List<String> getResponsesList() {
        return this.responses;
    }

    public String getTupleToTake() {

        Set<String> intersection = new HashSet<>(matchingTuples.get(0));

        for (int i = 1; i < matchingTuples.size(); i++) {
            intersection.retainAll(matchingTuples.get(i));
        }

        // Returns one of the tuples that was common to all
        // the matching tuples lists or null
        return intersection.stream().findFirst().orElse(null);
    }

}