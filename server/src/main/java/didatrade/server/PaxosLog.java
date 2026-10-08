package didatrade.server;

import java.util.Hashtable;
import java.util.ArrayList;
import java.util.List;

import didatrade.DidaTradePaxos;



public class PaxosLog {
    private Hashtable<Integer, PaxosInstance>  log;
     
    public PaxosLog() {
	this.log = new Hashtable<Integer, PaxosInstance>();
    }

    public synchronized int length() {
        return this.log.size();
    }
    
    public synchronized PaxosInstance getEntry(int position) {
        return this.log.get(position);
    }
   
   
    public synchronized PaxosInstance testAndSetEntry(int position) {
	PaxosInstance entry = this.log.get(position);

	if (entry == null){
	    entry = new PaxosInstance(position);
	    this.log.put (position, entry);
	}
	return entry;
    }


    public synchronized List<DidaTradePaxos.AcceptedEntry> acceptedFrom(int from) {
        List<DidaTradePaxos.AcceptedEntry> accepted = new ArrayList<DidaTradePaxos.AcceptedEntry>();
        for (PaxosInstance entry : this.log.values()) {
            if (entry.instance_nb >= from && entry.write_ballot > -1) {
                accepted.add(DidaTradePaxos.AcceptedEntry.newBuilder()
                    .setInstance(entry.instance_nb)
                    .setValue(entry.command_id)
                    .setValballot(entry.write_ballot)
                    .build());
            }
        }
        return accepted;
    }
}
