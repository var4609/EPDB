package org.epdb.app;

import org.epdb.engine.EngineModule;

public class Main {
    public static void main(String[] args) {
        System.out.println("*************************************************");
        System.out.println("** Database System Startup (Phase 1: Read Core) **");
        System.out.println("*************************************************");

        var tableName = "users";
        var db = EngineModule.INSTANCE.getQueryExecutor();
        db.populateTestData(tableName);
//      db.executeSelectQuery("users");
//      db.executeSelectQueryWithFilter("users");
        db.executeSelectQueryWithFilterAndProjection(tableName);
    }
}