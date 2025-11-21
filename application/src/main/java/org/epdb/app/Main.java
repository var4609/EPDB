package org.epdb.app;

import java.util.HashMap;

import org.epdb.buffer.InMemoryBufferManager;
import org.epdb.engine.database.Database;
import org.epdb.storage.manager.InMemoryStorageManager;

public class Main {
    public static void main(String[] args) {
        System.out.println("*************************************************");
        System.out.println("** Database System Startup (Phase 1: Read Core) **");
        System.out.println("*************************************************");
        
        var storageManager = new InMemoryStorageManager();
        var bufferManager = new InMemoryBufferManager(storageManager, 50, new HashMap<>());
        var db = new Database(bufferManager,  storageManager);
        db.populateTestData();
//        db.executeSelectQuery("users");
//        db.executeSelectQueryWithFilter("users");
        db.executeSelectQueryWithFilterAndProjection("users");
    }
}