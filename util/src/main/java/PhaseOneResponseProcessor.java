package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;

import didatrade.DidaTradePaxos;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
    private HashSet<Integer>         promises;
    private boolean                  rejected;
    private boolean                  has_quorum;
    private int                      value;
    private int                      valballot;
    private int                      maxballot;
    private int                      low_ballot;   
    private int                      high_ballot;

    public PhaseOneResponseProcessor (ConfigurationScheduler s, int l, int h) {
	this.promises    = new HashSet<Integer>();
	this.rejected    = false;
	this.has_quorum  = false;
	this.value       = -1;
	this.valballot   = -1;
	this.maxballot   = -1;
	this.low_ballot  = l;
	this.high_ballot = h;
	this.scheduler   = s;
    }

    public synchronized boolean getAccepted() {
	return (this.has_quorum && !this.rejected);
    }

    public synchronized int getValue() {
	return this.value;
    }

    public synchronized int getValballot() {
	return this.valballot;
    }

    public synchronized int getMaxballot() {
	return this.maxballot;
    }

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response) {
	
	if (last_response.getMaxballot() > this.maxballot)
	    this.maxballot = last_response.getMaxballot();

	
	if (last_response.getAccepted() == false) {
	    this.rejected = true;
	    return true;
	}

	this.promises.add(last_response.getServerid());

	if (last_response.getValballot() > this.valballot) {
	    this.valballot = last_response.getValballot();
	    this.value     = last_response.getValue();
	}

	if (this.promises.size() >= this.scheduler.quorum(this.high_ballot)) {
	    this.has_quorum = true;
	    return true;
	}

	return false;
    }
}