package didatrade.server;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneResponseProcessor;
//import didatrade.util.PhaseOneBogusProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

import didatrade.configs.ConfigurationScheduler;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class MainLoop implements Runnable  {
    DidaTradeServerState server_state;

    private boolean has_work;
    private AtomicInteger next_log_entry;
    private List<Integer> all_participants;
    private int n_participants;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;
    
    
    public MainLoop(DidaTradeServerState state) {
	this.server_state   = state;
	this.has_work       = false;
	this.next_log_entry = new AtomicInteger(-1);
    }

    public void run() {
	while (true) {
		this.server_state.waitIfFrozen();
		int ballot = this.server_state.getCurrentBallot();

		if (server_state.scheduler.leader(ballot) != this.server_state.my_id) {
			waitForWork();
			continue;
		}

		if(!server_state.isPreparedFor(ballot)){
			if (ballot == 0 && this.server_state.req_history.getFirstPending() == null) {
				waitForWork();
				continue;
			}
			takeOverAsLeader(ballot);
			continue;
		}

		RequestRecord request = this.server_state.req_history.takeFirstPending();
		if (request == null) {
    		waitForWork();
    		continue;
		}

		if (this.server_state.req_history.getIfProcessed(request.getId()) != null) {
    		continue;
}
		int instance = allocateNextInstance();
		InstanceWorker worker = new InstanceWorker(this.server_state, instance, request);
		new Thread(worker).start();
	}
    }

	private synchronized void waitForWork() {
		while (!this.has_work) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
		this.has_work = false;
	}

	private int allocateNextInstance() {
		int applied = server_state.applier.getNextApply();
		if (applied - 1 > this.next_log_entry.get()) {
			this.next_log_entry.set(applied - 1);
		}
		return this.next_log_entry.incrementAndGet();
	}
      
    public synchronized void wakeup() {
	this.has_work = true;
	notify();    
    }

	private void takeOverAsLeader(int ballot){
		int from = server_state.applier.getNextApply();

		InstanceWorker first_cleanup = new InstanceWorker(this.server_state, from, null);
		first_cleanup.run();

		if (server_state.getCurrentBallot() != ballot){
			return;
		}

		int remote = first_cleanup.getLearnedMaxInstance();
		int local = server_state.paxos_log.highestInstance();
		int horizon = Math.max(remote, local) + 1;

		List<Thread> workers = new ArrayList<Thread>();
		for (int i = from + 1; i < horizon; i++) {
			Thread t = new Thread(new InstanceWorker(this.server_state, i, null));
			workers.add(t);
			t.start();
		}

		for (Thread t : workers) {
			try {
				t.join();
			} catch (InterruptedException e) {
			}
		}

		if(server_state.getCurrentBallot() != ballot){
			return;
		}

		if (horizon > this.next_log_entry.get()) {
			this.next_log_entry.set(horizon - 1);
		}

		server_state.markPrepared(ballot, horizon);
	}
}
