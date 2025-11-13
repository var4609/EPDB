package org.epdb.engine.database;

import java.util.Arrays;
import java.util.Set;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.comparison.ComparisonPredicate;
import org.epdb.engine.comparison.Op;
import org.epdb.engine.dto.*;
import org.epdb.engine.volcano.Insert;
import org.epdb.engine.volcano.Projection;
import org.epdb.engine.volcano.Selection;
import org.epdb.engine.volcano.TableScan;
import org.epdb.storage.StorageManager;

public class Database {

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

        var scanOperator = new TableScan(bufferManager, schema, USERS_TABLE_START_PAGE);

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

    public void executeSelectQueryWithFilter(String tableName) {
        if(!tableName.equals("users")) {
            System.out.println("Table not found: " + tableName);
            return;
        }

        var scanOperator = new TableScan(bufferManager, schema, USERS_TABLE_START_PAGE);
        var predicate = new ComparisonPredicate(0, Op.GREATER_THAN, 150);
        var filterOperator = new Selection(predicate, scanOperator);

        System.out.println("\n--- Query Execution: SELECT * FROM users WHERE id > 150 ---");
        System.out.println(Arrays.toString(schema.columnNames()));
        System.out.println("---------------------------------------------");

        filterOperator.open();
        Tuple tuple;
        while((tuple = filterOperator.next()) != null) {
            System.out.println(tuple);
        }

        filterOperator.close();
    }

    public void executeSelectQueryWithFilterAndProjection(String tableName) {
        if(!tableName.equals("users")) {
            System.out.println("Table not found: " + tableName);
            return;
        }

        var scanOperator = new TableScan(bufferManager, schema, USERS_TABLE_START_PAGE);
        var predicate = new ComparisonPredicate(0, Op.GREATER_THAN, 150);
        var filterOperator = new Selection(predicate, scanOperator);
        var projectionOperator = new Projection(filterOperator, Set.of(0, 1));

        System.out.println("\n--- Query Execution: SELECT id, name FROM users WHERE id > 150 ---");
        System.out.println(Arrays.toString(schema.columnNames()));
        System.out.println("---------------------------------------------");

        projectionOperator.open();
        Tuple tuple;
        while((tuple = projectionOperator.next()) != null) {
            System.out.println(tuple);
        }

        projectionOperator.close();
    }

    public void executeInsert(Tuple tupleToInsert) {

        Insert insertOperator = new Insert(
                bufferManager,
                tupleToInsert,
                USERS_TABLE_START_PAGE
        );

        insertOperator.open();
        insertOperator.next();
        insertOperator.close();
    }

    public void populateTestData() {
        System.out.println("\n--- Admin: Populating Test Data Directly to Storage ---");
        var rowCount = 500;

        for(var i=0; i<rowCount; i++) {
            var id = new IntValue(i + 100);
            var name = new StringValue("User_" + i);
            var age = new IntValue(20 + i);

            executeInsert(new Tuple(new ColumnValue[]{id, name, age}));
        }
    }
}
