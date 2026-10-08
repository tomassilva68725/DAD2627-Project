package didatrade.server;

import java.util.ArrayList;
import java.util.List;

import didatrade.DidaTradePaxos;
import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseTwoResponseProcessor;

public class InstanceWorker implements Runnable {
    private DidaTradeServerState server_state;
    private int instance;
    private RequestRecord request;
    private int forced_value;
    public static final int NO_OP = -1;

    public InstanceWorker(DidaTradeServerState state, int instance, RequestRecord request) {
        this(state, instance, request, NO_OP);
    }

    public InstanceWorker(DidaTradeServerState state, int instance, RequestRecord request, int forced_value) {
        this.server_state = state;
        this.instance     = instance;
        this.request      = request;
        this.forced_value = forced_value;
    }


    public void run() {
        boolean decided = false;

        while (!decided) {
            this.server_state.waitIfFrozen();
            int ballot = this.server_state.getCurrentBallot();

            if(server_state.scheduler.leader(ballot) != server_state.my_id) {
                // Not the leader
                giveBackRequest();
                return;
            }

            if (!server_state.isPreparedFor(ballot)) {
                // ballot mudou e ainda ninguém fez fase 1 para ele: o MainLoop trata disso
                giveBackRequest();
                return;
            }

            List<Integer> acceptors = server_state.scheduler.acceptors(ballot);
            int quorum = server_state.scheduler.quorum(ballot);
            int n_acceptors = acceptors.size();

            int value = ownValue();

            System.out.println("[" + System.currentTimeMillis() + "] Instance " + this.instance + ": starting phase 2 with value " + value + " ballot " + ballot);

            //Phase 2
            DidaTradePaxos.PhaseTwoRequest.Builder phase_two_request_builder = DidaTradePaxos.PhaseTwoRequest.newBuilder();
            phase_two_request_builder.setInstance(this.instance);
            phase_two_request_builder.setRequestballot(ballot);
            phase_two_request_builder.setValue(value);
            DidaTradePaxos.PhaseTwoRequest phase_two_request = phase_two_request_builder.build();

            PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);
            ArrayList<DidaTradePaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaTradePaxos.PhaseTwoReply>();
            GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> phase_two_collector =
                new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);

            for (int i = 0; i < n_acceptors; i++) {
                CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> phase_two_observer =
                    new CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply>(phase_two_collector);
                server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request, phase_two_observer);
            }
            phase_two_collector.waitUntilDone();

            if (phase_two_processor.getAccepted() == false) {
                server_state.setCurrentBallot(phase_two_processor.getMaxballot());
                try { Thread.sleep(100); } catch (InterruptedException e) {}
                continue;
            }

            System.out.println("[" + System.currentTimeMillis() + "] Instance " + this.instance + ": DECIDED with value " + value);
            markDecided(value);

            decided = true;
        }
    }

    private void markDecided(int value) {
        PaxosInstance entry = server_state.paxos_log.testAndSetEntry(this.instance);
        synchronized (entry) {
            if (!entry.decided) {
                entry.command_id = value;
                entry.decided    = true;
            }
            entry.notifyAll();
        }
    }

    private void giveBackRequest() {
        if (this.request == null)
            return;
        server_state.req_history.requeue(this.request.getId());
        server_state.main_loop.wakeup();
    }

    private int ownValue() {
        return (this.request != null) ? this.request.getId() : this.forced_value;
    }
}
