package com.mapr.cell.spark;


import com.mapr.cell.common.DAO;
import com.mapr.db.Table;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.ojai.Document;

public class MaprDBReceiver extends Receiver<String> {
    private Table table;
    private Thread thread;

    public MaprDBReceiver() {
        super(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        table = new DAO().getCdrsTable();
        thread = new Thread("Socket Receiver") {
            @Override
            public void run() {
                while (!this.isInterrupted()) {
                    receive();
                }
            }
        };
        thread.start();
    }

    private void receive() {
        for (Document document : table.find()) {
            store(document.toString());
        }
    }

    @Override
    public void onStop() {
        thread.interrupt();
    }
}
