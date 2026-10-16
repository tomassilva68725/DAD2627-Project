package didatrade.server;

import java.util.concurrent.atomic.AtomicInteger;

public class Applier implements Runnable {
    public static final int STALL_TIMEOUT_MS = 3000;
    private DidaTradeServerState server_state;
    private AtomicInteger next_apply;
    private int executed;

    public Applier(DidaTradeServerState state) {
        this.server_state = state;
        this.next_apply   = new AtomicInteger(0);
        this.executed     = 0;
    }

    public int getNextApply() {
        return this.next_apply.get();
    }

    public void run() {
        while (true){
            this.server_state.waitIfFrozen();
            PaxosInstance entry = server_state.paxos_log.testAndSetEntry(this.next_apply.get());
        
            int ballot = server_state.getCurrentBallot();
            boolean decided;
            synchronized (entry) {
                if (!entry.decided) {
                    try { entry.wait(STALL_TIMEOUT_MS); } catch (InterruptedException e) {}
                }
                decided = entry.decided;
            }
            if (!decided) {
                // Fast Paxos: sem decisão nem colisão visível ao fim do timeout -> recuperação no ballot seguinte
                if (server_state.getCurrentBallot() == ballot && server_state.isFast(ballot)
                    && server_state.req_history.getFirstPending() != null) {
                    System.out.println("Fast Paxos timeout on instance " + this.next_apply.get() + " -> moving to ballot " + (ballot + 1));
                    server_state.setCurrentBallot(ballot + 1);
                    server_state.main_loop.wakeup();
                }
                continue;
            }
        applyEntry(entry.command_id, this.next_apply.get() + 1);
        this.next_apply.incrementAndGet();
        }
    }

    private void applyEntry(int command_id, int index) {
        if(command_id == InstanceWorker.NO_OP) {
            markExecuted(index);
            return;
        }
        
        while(server_state.req_history.getIfProcessed(command_id) == null) {
            RequestRecord request_record = server_state.req_history.getIfPending(command_id);
            if (request_record != null) {
                executeCommand(request_record, index);
                server_state.req_history.moveToProcessed(command_id);
                return;
            }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        }
        markExecuted(index); 
    }

    // Step 4: avança o contador de instâncias executadas 
    private void markExecuted(int index) {
        synchronized (server_state.trade_manager) {
            if (index > this.executed)
                this.executed = index;
            server_state.trade_manager.notifyAll();
        }
    }

    // Step 4: balance SEM Paxos. Espera até esta réplica ter executado pelo menos 'lastseen' instâncias *rever
    public int[] causalBalance(int user_id, int lastseen) {
        synchronized (server_state.trade_manager) {
            while (this.executed < lastseen) {
                try {
                    server_state.trade_manager.wait();
                } catch (InterruptedException e) {}
            }
            return new int[] { server_state.trade_manager.balance(user_id), this.executed };
        }
    }
     
    private void executeCommand(RequestRecord request_record, int index) {
        DidaTradeCommand command = request_record.getRequest();
        boolean result = false;
        int balance = 0;

        synchronized (server_state.trade_manager) {
            switch (command.getAction()) {
                case DidaTradeAction.POPULATE:
                    result = server_state.trade_manager.populate(command.getQuantity());
                    break;
                case DidaTradeAction.ADDUSER:
                    result = server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock());
                    break;
                case DidaTradeAction.SELL:
                    result = server_state.trade_manager.sell(command.getUserId(), command.getQuantity());
                    break;
                case DidaTradeAction.BUY:
                    result = server_state.trade_manager.acquire(command.getUserId(), command.getQuantity());
                    break;
                case DidaTradeAction.BALANCE:
                    balance = server_state.trade_manager.balance(command.getUserId());
                    command.setQuantity(balance);
                    result = (balance != -1);
                    break;
                case DidaTradeAction.DUMP:
                    server_state.trade_manager.dump();
                    result = true;
                    break;
                default:
                    result = false;
                    System.err.println("*** Unknown command ****");
                    break;
            }
        
        if (index > this.executed)
            this.executed = index;
        server_state.trade_manager.notifyAll();
        }
        
        request_record.setResponse(result, index);
    }
}
