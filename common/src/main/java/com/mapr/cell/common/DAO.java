package com.mapr.cell.common;


import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.store.DocumentMutation;

import java.util.Date;




public class DAO {

    public static final String SIMULATION_KEY = "SIMULATION_KEY";
    private static volatile DAO instance;

    public Table getCdrsTable() {
        return cdrsTable;
    }
    public Table getStatsTable() {
        return statsTable;
    }
    public Table getSimulationTable() {
        return simulationTable;
    }

    private Table statsTable;
    private Table cdrsTable;
    private Table simulationTable;

    private final static String APPS_DIR = "/apps/telco/db/";
    private static final String CDRS_TABLE = APPS_DIR + "cdrs";
    private static final String STATS_TABLE = APPS_DIR + "stats";
    private static final String SIMULATION_TABLE = APPS_DIR + "simul";

    private DAO() {
        this.cdrsTable = this.getTable(CDRS_TABLE);
        this.statsTable = this.getTable(STATS_TABLE);
        this.simulationTable = this.getTable(SIMULATION_TABLE);
    }

    public static DAO getInstance() {
        if (instance == null)
            synchronized (DAO.class){
                if (instance == null)
                    instance = new DAO();
            }
        return instance;
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

    public void newSimulation(){
        DocumentMutation mutation = MapRDB.newMutation().
                setOrReplace("time", new Date().getTime()).
                increment("simulationId", 1);
        simulationTable.update(SIMULATION_KEY, mutation);
    }

    public long getLastSimulationID() {
        return simulationTable.findById(SIMULATION_KEY).getInt("simulationId");
    }

    public void addCDR(String cdrJson) {
        Document document = MapRDB.newDocument(cdrJson);
        document.set("emulationId", getLastSimulationID());
        System.out.println(document);
        //TODO: add zeros to constant length (for dzhuribeda)
        cdrsTable.insert("00000"+document.getDouble("time") +"/"+ document.getString("callerId")+"/"
                + document.getString("towerId"), document);
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
