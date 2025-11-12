package org.epdb.engine.database;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.Schema;
import org.epdb.engine.dto.Tuple;
import org.epdb.engine.volcano.TableScan;
import org.epdb.storage.StorageManager;

public class Database {
    
    private static final Long PAGE_SIZE = 4096L;
    private static final int ROW_SIZE = 28;
    private static final Long USERS_TABLE_START_PAGE = 0L;

    private final BufferManager bufferManager;
    private final StorageManager storageManager;
    private final Schema schema;

    public Database(BufferManager bufferManager, StorageManager storageManager) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
        this.storageManager.allocateNewPage();
        this.storageManager.allocateNewPage();
        this.storageManager.allocateNewPage();
        this.schema = new Schema(new String[]{"id", "name", "age"});
    }

    public void executeSelectQuery(String tableName) {
        if(!tableName.equals("users")) {
            System.out.println("Table not found: " + tableName);
            return;
        }

        var scanOperator = new TableScan(bufferManager, schema, (long) USERS_TABLE_START_PAGE);

        System.out.println("\n--- Query Execution: SELECT * FROM users ---");
        System.out.println(Arrays.toString(schema.columnNames()));
        System.out.println("---------------------------------------------");

        scanOperator.open();
        Tuple tuple;
        while((tuple = scanOperator.next()) != null) {
            System.out.println(tuple);
        }

        scanOperator.close();
    }

    public void populateTestData() {
        System.out.println("\n--- Admin: Populating Test Data Directly to Storage ---");

        var pageId = USERS_TABLE_START_PAGE;
        var page = bufferManager.getPage(pageId);
        var byteBuffer = ByteBuffer.wrap(page.data());
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        var rowOffset = 0;
        var rowCount = 500;

        for(var i=0; i<rowCount; i++) {
            if(rowOffset + ROW_SIZE > PAGE_SIZE) {
                break;
            }

            var name = "User_" + i;
            var nameBytes = name.getBytes(StandardCharsets.UTF_8);

            byteBuffer.position(rowOffset);
            byteBuffer.putInt(i + 100);

            byteBuffer.put(nameBytes, 0, Math.min(nameBytes.length, 20));

            byteBuffer.position(rowOffset + 4 + 20);
            byteBuffer.putInt(20 + i);

            rowOffset += ROW_SIZE;
        }

        storageManager.writePage(pageId, byteBuffer.array());
    }
}
