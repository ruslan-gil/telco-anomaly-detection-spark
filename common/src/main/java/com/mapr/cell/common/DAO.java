package com.mapr.cell.common;


import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.store.exceptions.DocumentExistsException;

public class DAO {
    public Table getCdrsTable() {
        return cdrsTable;
    }
    public Table getStatsTable() {
        return statsTable;
    }

    private Table statsTable;
    private Table cdrsTable;

    private final static String APPS_DIR = "/apps/telco/db/";
    private static final String CDRS_TABLE = APPS_DIR + "cdrs";
    private static final String STATS_TABLE = APPS_DIR + "stats";

    public DAO() {
        this.cdrsTable = this.getTable(CDRS_TABLE);
        this.statsTable = this.getTable(STATS_TABLE);
    }


    private static final Object lock = new Object();
    private Table getTable(String tableName) {
        Table table;
        System.out.println("Check DB");
        synchronized (lock) {
            if (!MapRDB.tableExists(tableName)) {
                table = MapRDB.createTable(tableName); // Create the table if not already present
            } else {
                table = MapRDB.getTable(tableName); // get the table
            }
        }

        return table;
    }

    public void addCDR(String cdrJson) {
        Document document = MapRDB.newDocument(cdrJson);
        System.out.println(document);
        cdrsTable.insert("00000"+document.getDouble("time") +"/"+ document.getString("callerId")+"/"+ document.getString("towerId"), document);
        System.out.println("inserted " + document.toString());
        cdrsTable.flush();
    }

    public void sendTowerStats(String tower, Document stats) {
        statsTable.insertOrReplace(tower, stats);
        statsTable.flush();
    }

    public String getCdrsTablePath(){
        return CDRS_TABLE;
    }
  }
