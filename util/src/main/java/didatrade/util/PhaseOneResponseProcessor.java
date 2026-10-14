package didatrade.util;

import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import didatrade.DidaTradePaxos;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
	private HashMap<Integer, DidaTradePaxos.AcceptedEntry> responses;
	private HashMap<Integer, List<DidaTradePaxos.AcceptedEntry>> all_entries;
    private boolean                  rejected;
    private boolean                  has_quorum;
    private int                      low_ballot;   
    private int                      high_ballot;
	private int maxballot;
	private HashMap<Integer, List<Integer>> configuration_acceptors;

    public PhaseOneResponseProcessor (ConfigurationScheduler s, int l, int h) {
	this.responses   = new HashMap<Integer, DidaTradePaxos.AcceptedEntry>();
	this.all_entries = new HashMap<Integer, List<DidaTradePaxos.AcceptedEntry>>();
	this.rejected    = false;
	this.has_quorum  = false;
	this.maxballot   = -1;
	this.low_ballot  = l;
	this.high_ballot = h;
	this.scheduler   = s;
	this.configuration_acceptors = new HashMap<Integer, List<Integer>>();


	for (Integer b = this.low_ballot; b <= this.high_ballot; b++){
		this.configuration_acceptors.put(b, new ArrayList<Integer>());
	}

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

	public synchronized HashMap<Integer, List<DidaTradePaxos.AcceptedEntry>> getAllEntries() {
		return this.all_entries;
	}

	public synchronized int getPromisesInConfig(int b) {
    	return this.configuration_acceptors.get(b).size();
	}

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response) {
	
	if (last_response.getMaxballot() > this.maxballot)
	    this.maxballot = last_response.getMaxballot();

	
	if (last_response.getAccepted() == false) {
	    this.rejected = true;
	    return true;
	}

	for (Integer b = low_ballot; b <= high_ballot; b++ ){
		Integer server_id = last_response.getServerid();

		if (this.scheduler.isacceptor(server_id, b)){
			this.configuration_acceptors.get(b).add(server_id);
		}
	}

	for (DidaTradePaxos.AcceptedEntry entry : last_response.getEntriesList()) {
		DidaTradePaxos.AcceptedEntry current = this.responses.get(entry.getInstance());
	    if (current == null || entry.getValballot() > current.getValballot()) {
	        this.responses.put(entry.getInstance(), entry);
	    }
		List<DidaTradePaxos.AcceptedEntry> list = this.all_entries.get(entry.getInstance());
		if (list == null) {
			list = new ArrayList<>();
			this.all_entries.put(entry.getInstance(), list);
		}
		list.add(entry);
	}


		for (Integer b : this.configuration_acceptors.keySet()){
			if (this.configuration_acceptors.get(b).size() < this.scheduler.quorum(b)){
				return false;
			}
		}
		this.has_quorum = true;
		return true;
	}
}