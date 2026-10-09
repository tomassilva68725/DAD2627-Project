package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import didatrade.DidaTradePaxos;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
    private HashSet<Integer>         promises;
	private HashMap<Integer, DidaTradePaxos.AcceptedEntry> responses;
    private boolean                  rejected;
    private boolean                  has_quorum;
    private int                      low_ballot;   
    private int                      high_ballot;
	private int maxballot;

    public PhaseOneResponseProcessor (ConfigurationScheduler s, int l, int h) {
	this.promises    = new HashSet<Integer>();
	this.responses   = new HashMap<Integer, DidaTradePaxos.AcceptedEntry>();
	this.rejected    = false;
	this.has_quorum  = false;
	this.maxballot   = -1;
	this.low_ballot  = l;
	this.high_ballot = h;
	this.scheduler   = s;
    }

    public synchronized boolean getAccepted() {
	return (this.has_quorum && !this.rejected);
    }

	public synchronized int getMaxBallot() {
		return this.maxballot;
	}

	public synchronized HashMap<Integer, DidaTradePaxos.AcceptedEntry> getResponses() {
		return this.responses;
	}


    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response) {
	
	if (last_response.getMaxballot() > this.maxballot)
	    this.maxballot = last_response.getMaxballot();

	
	if (last_response.getAccepted() == false) {
	    this.rejected = true;
	    return true;
	}

	this.promises.add(last_response.getServerid());

	for (DidaTradePaxos.AcceptedEntry entry : last_response.getEntriesList()) {
		DidaTradePaxos.AcceptedEntry current = this.responses.get(entry.getInstance());
	    if (current == null || entry.getValballot() > current.getValballot()) {
	        this.responses.put(entry.getInstance(), entry);
	    }
	}


	if (this.promises.size() >= this.scheduler.quorum(this.high_ballot)) {
	    this.has_quorum = true;
	    return true;
	}

	return false;
    }
}