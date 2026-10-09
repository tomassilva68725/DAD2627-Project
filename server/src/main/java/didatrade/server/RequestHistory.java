
package didatrade.server;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.ArrayDeque;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

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
     * @return the first pending request record, or null if there are no pending requests
     */
    public synchronized RequestRecord takeFirstPending() {
        Integer id = this.pending_req_ids_queue.poll();
        if (id != null) {
            return this.pending.get(id);
        } else {
            return null;
        }
    }

    public synchronized void requeue(int requestid) {
        Integer id = new Integer(requestid);
        if (this.pending.containsKey(id) && !this.pending_req_ids_queue.contains(id))
            this.pending_req_ids_queue.addFirst(id);
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

    public synchronized List<Integer> pendingIdsSorted() {
        List<Integer> ids = new ArrayList<Integer>(this.pending.keySet());
        Collections.sort(ids);
        return ids;
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
    this.pending_req_ids_queue.remove(id);
	return record;
    }
   
        
}
