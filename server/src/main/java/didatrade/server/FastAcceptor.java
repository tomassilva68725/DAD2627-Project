package didatrade.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import didatrade.DidaTradePaxos;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;

import io.grpc.Context;


public class FastAcceptor {
    private DidaTradeServerState server_state;
    private int any_ballot;                 
    private int next_fast_instance;         
    private HashSet<Integer> assigned;      

    public FastAcceptor(DidaTradeServerState state) {
        this.server_state       = state;
        this.any_ballot         = -1;
        this.next_fast_instance = 0;
        this.assigned           = new HashSet<Integer>();
    }

    
    public int onAny(int ballot, int from) {
        List<int[]> to_notify = new ArrayList<int[]>();
        int maxballot;
        synchronized (server_state.acceptor_lock) {
            if (ballot < server_state.getCurrentBallot())
                return server_state.getCurrentBallot();
            server_state.setCurrentBallot(ballot);
            this.any_ballot         = ballot;
            this.next_fast_instance = from;
            this.assigned.clear();
            // pedidos que já tinham chegado antes do ANY
            for (int reqid : server_state.req_history.pendingIdsSorted()) {
                int[] vote = assignLocked(reqid);
                if (vote != null)
                    to_notify.add(vote);
            }
            maxballot = server_state.getCurrentBallot();
        }
        System.out.println("Fast acceptor: ANY ballot " + ballot + " from instance " + from + " (" + to_notify.size() + " pedidos pendentes atribuidos)");
        for (int[] v : to_notify)
            notifyLearners(v[0], v[1], v[2]);
        return maxballot;
    }

    
    public void onClientRequest(int reqid) {
        int[] vote;
        synchronized (server_state.acceptor_lock) {
            vote = assignLocked(reqid);
        }
        if (vote != null)
            notifyLearners(vote[0], vote[1], vote[2]);
    }

    
    private int[] assignLocked(int reqid) {
        if (this.any_ballot < 0 || server_state.getCurrentBallot() != this.any_ballot)
            return null;
        if (this.assigned.contains(reqid) || server_state.req_history.getIfProcessed(reqid) != null)
            return null;
        int instance = this.next_fast_instance++;
        PaxosInstance entry = server_state.paxos_log.testAndSetEntry(instance);
        synchronized (entry) {
            entry.accepted_value = reqid;
            entry.write_ballot   = this.any_ballot;
        }
        this.assigned.add(reqid);
        System.out.println("Fast acceptor: pedido " + reqid + " -> instancia " + instance + " (ballot " + this.any_ballot + ")");
        return new int[] { instance, this.any_ballot, reqid };
    }


    private void notifyLearners(int instance, int ballot, int value) {
        Context.current().fork().run(() -> sendLearn(instance, ballot, value));
    }

    private void sendLearn(int instance, int ballot, int value) {
        List<Integer> learners = server_state.scheduler.learners(ballot);
        int n_targets = learners.size();
        DidaTradePaxos.LearnRequest learn_request = DidaTradePaxos.LearnRequest.newBuilder()
            .setInstance(instance)
            .setBallot(ballot)
            .setValue(value)
            .setServerid(server_state.my_id)
            .setFast(true)
            .build();
        ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
        GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector =
            new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);
        for (int i = 0; i < n_targets; i++) {
            CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer =
                new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
            server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
        }
    }
}