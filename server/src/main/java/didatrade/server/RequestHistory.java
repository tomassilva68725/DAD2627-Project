
package didatrade.server;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.ArrayDeque;

public class RequestHistory {
    private Hashtable<Integer, RequestRecord> pending;
    private Hashtable<Integer, RequestRecord> processed;
    
    // A queue to keep track of the order of pending requests
    private ArrayDeque<Integer> pending_req_ids_queue;

    public RequestHistory() {
	this.pending = new Hashtable<Integer, RequestRecord>();
        this.processed = new Hashtable<Integer, RequestRecord>();
        this.pending_req_ids_queue = new ArrayDeque<Integer>();
    }

    public synchronized RequestRecord getIfPending(int requestid) {
	Integer id = new Integer(requestid);
        return this.pending.get(id);
    }
   
    public synchronized RequestRecord getFirstPending() {
	Enumeration<Integer> pendingids = this.pending.keys();
        if (pendingids.hasMoreElements()) 
            return this.pending.get(pendingids.nextElement());
	else
	    return null;
    }

    /**
     * Takes the first pending request and removes it from the pending list.
     * btw, do not forget to save the request in a local variable at mainLoop
     * because it will be removed from the pending list.
     * @return the first pending request record, or null if there are no pending requests
     */
    public synchronized RequestRecord takeFirstPending() {
        Integer id = this.pending_req_ids_queue.poll();
        if (id != null) {
            return this.pending.remove(id);
        } else {
            return null;
        }
    }
   
    public synchronized RequestRecord getIfProcessed(int requestid) {
	Integer id = new Integer(requestid);
        return this.processed.get(id);
    }
   
    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record;
	Integer id = new Integer(requestid);

	record = this.pending.get(id);
	if (record == null)
	    record = this.processed.get(id);
	return record;
    }
   
    public synchronized void addToPending(int requestid, RequestRecord record) {
	Integer id = new Integer(requestid);
	
	this.pending.put (id, record);
    this.pending_req_ids_queue.offer(id);
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
	Integer id = new Integer(requestid);
        RequestRecord record = this.pending.remove(id);
	this.processed.put (id, record);
	return record;
    }
   
        
}
