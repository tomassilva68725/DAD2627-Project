package didatrade.server;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneResponseProcessor;

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

		// Fast Paxos: num ballot F o líder não propõe pedidos; são os acceptors que os aceitam diretamente dos clientes
		if (server_state.isFast(ballot)) {
			waitForWork();
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
		InstanceWorker worker = new InstanceWorker(this.server_state, ballot, instance, request);
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

	// Multi-Paxos: uma unica fase 1 por ballot, cobrindo todas as instancias a partir de "from"
	private void takeOverAsLeader(int ballot) {
		int from = server_state.applier.getNextApply();
		List<Integer> acceptors = server_state.scheduler.acceptors(ballot);
		int n_acceptors = acceptors.size();

		DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest.newBuilder()
			.setInstance(from)
			.setRequestballot(ballot)
			.build();

		int low_ballot = Math.max(server_state.getCompletedBallot(), 0);
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(server_state.scheduler, low_ballot, ballot);
		ArrayList<DidaTradePaxos.PhaseOneReply> responses = new ArrayList<DidaTradePaxos.PhaseOneReply>();
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector =
			new GenericResponseCollector<DidaTradePaxos.PhaseOneReply>(responses, n_acceptors, processor);

		for (int i = 0; i < n_acceptors; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> observer =
				new CollectorStreamObserver<DidaTradePaxos.PhaseOneReply>(collector);
			server_state.async_stubs[acceptors.get(i)].phaseone(request, observer);
		}
		collector.waitUntilDone();

		if (!processor.getAccepted()) {
			server_state.setCurrentBallot(processor.getMaxBallot());
			try { Thread.sleep(100); } catch (InterruptedException e) {}
			return;
		}

		HashMap<Integer, DidaTradePaxos.AcceptedEntry> accepted = processor.getResponses();
		HashMap<Integer, List<DidaTradePaxos.AcceptedEntry>> all_entries = processor.getAllEntries();
		int n_promises = processor.getPromiseCount();
		int horizon = from;
		for (int inst : accepted.keySet())
			horizon = Math.max(horizon, inst + 1);

		System.out.println("[" + System.currentTimeMillis() + "] Takeover ballot " + ballot + ": phase 1 done, re-proposing [" + from + ", " + horizon + ")");

		this.next_log_entry.set(horizon - 1);

		// tem de ser antes dos workers, senão o guard isPreparedFor manda-os embora
		server_state.markPrepared(ballot);

		List<Thread> workers = new ArrayList<Thread>();
		for (int i = from; i < horizon; i++) {
			int value = chooseValue(all_entries.get(i), n_promises);
			Thread t = new Thread(new InstanceWorker(this.server_state, ballot, i, null, value));
			workers.add(t);
			t.start();
		}

		// Fast Paxos: a partir do horizon os acceptors aceitam valores diretamente dos clientes
		if (server_state.isFast(ballot))
			sendAny(ballot, horizon);

		for (Thread t : workers) {
			try { t.join(); } catch (InterruptedException e) {}
		}
	}

	private int chooseValue(List<DidaTradePaxos.AcceptedEntry> entries, int n_promises) {
		if (entries == null || entries.isEmpty())
			return InstanceWorker.NO_OP;
		int k = -1;
		for (DidaTradePaxos.AcceptedEntry e : entries)
			k = Math.max(k, e.getValballot());
		HashMap<Integer, Integer> count = new HashMap<Integer, Integer>();
		for (DidaTradePaxos.AcceptedEntry e : entries)
			if (e.getValballot() == k)
				count.merge(e.getValue(), 1, Integer::sum);
		if (!server_state.isFast(k))
			return count.keySet().iterator().next();
		int f = server_state.scheduler.acceptors(k).size() - server_state.scheduler.fastquorum(k);
		int best = InstanceWorker.NO_OP, best_count = -1;
		for (Map.Entry<Integer, Integer> c : count.entrySet()) {
			if (n_promises - c.getValue() <= f)
				return c.getKey();                       // pode ter sido decidido no ballot rápido k: obrigatório
			if (c.getValue() > best_count) {
				best = c.getKey();
				best_count = c.getValue();
			}
		}
		return best;
	}

	private void sendAny(int ballot, int from) {
		System.out.println("[" + System.currentTimeMillis() + "] Leader: ballot " + ballot + " is FAST -> sending ANY from instance " + from);
		DidaTradePaxos.PhaseTwoRequest any = DidaTradePaxos.PhaseTwoRequest.newBuilder()
			.setInstance(from)
			.setRequestballot(ballot)
			.setValue(InstanceWorker.NO_OP)
			.setAny(true)
			.build();
		for (int a : server_state.scheduler.acceptors(ballot)) {
			GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector =
				new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(new ArrayList<DidaTradePaxos.PhaseTwoReply>(), 1);
			server_state.async_stubs[a].phasetwo(any, new CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply>(collector));
		}
	}
 
}
