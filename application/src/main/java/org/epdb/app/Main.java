package org.epdb.app;

import org.epdb.engine.Engine;

public class Main {
    public static void main(String[] args) {
        System.out.println("Starting EPDB...");
        Engine engine = new Engine();
        engine.insertIntoTable();
    }
}