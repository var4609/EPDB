package org.epdb.app;

import java.util.HashMap;

import org.epdb.buffer.BufferManager;
import org.epdb.buffer.InMemoryBufferManager;
import org.epdb.buffer.dto.BufferFrame;
import org.epdb.engine.database.Database;
import org.epdb.storage.InMemoryStorageManager;
import org.epdb.storage.StorageManager;

public class Main {
    public static void main(String[] args) {
        System.out.println("*************************************************");
        System.out.println("** Database System Startup (Phase 1: Read Core) **");
        System.out.println("*************************************************");
        
        StorageManager storageManager = new InMemoryStorageManager(); 
        BufferManager bufferManager = new InMemoryBufferManager(storageManager, 10, new HashMap<Long, BufferFrame>()); 
        Database db = new Database(bufferManager,  storageManager);
        db.populateTestData();
        db.executeSelectQuery("users");
    }
}