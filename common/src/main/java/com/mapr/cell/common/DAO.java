package com.mapr.cell.common;


import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;

public class DAO {
    Table cdrsTable;

    private final static String APPS_DIR = "/apps/telco/db/";
    private static final String CDRS_TABLE = APPS_DIR + "cdrs";

    public DAO() {
        this.cdrsTable = this.getTable(CDRS_TABLE);
    }


    private static final Object lock = new Object();
    private Table getTable(String tableName) {
        Table table;
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
        cdrsTable.insert("00000"+document.getString("time") +"/"+ document.getString("callerId"), document);

        cdrsTable.flush();
    }
  }
