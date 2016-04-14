package com.mapr.cell.spark;


import com.mapr.cell.common.DAO;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.ojai.Document;
import org.ojai.store.QueryCondition;

public abstract class MaprDBReceiver extends Receiver<String> {
    private Table table;
    private Thread thread;

    public MaprDBReceiver() {
        super(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        table = getTable();
        thread = new Thread("Socket Receiver") {
            @Override
            public void run() {
                while (!this.isInterrupted()) {
                    receive();
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
    }

    private void receive() {
        long lastId = DAO.getInstance().getLastSimulationID();
        QueryCondition c = MapRDB.newCondition().is("simulationId", QueryCondition.Op.EQUAL, lastId);
        for (Document document : table.find(c)) {
            store(document.toString());
        }
    }

    @Override
    public void onStop() {
        thread.interrupt();
    }

    protected abstract Table getTable();

    public static class CDRReceiver extends MaprDBReceiver {

        @Override
        protected Table getTable() {
            return DAO.getInstance().getCdrsTable();
        }
    }

    public static class StatsReceiver extends MaprDBReceiver {

        @Override
        protected Table getTable() {
            return DAO.getInstance().getStatsTable();
        }
    }
}
