package com.aerodb;

import com.aerodb.buffer.BufferManager;
import com.aerodb.index.BTreeFile;
import com.aerodb.storage.*;

import javax.swing.*;
import java.io.File;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("=== JDB Storage Engine Starting ===");

        // 1. Setup Phase (Initialization)
        File dbFile = new File("data.db");
        File indexFile = new File("index.db");
        boolean isExistingDb = dbFile.exists();

        // Define Schema
        TupleDesc schema = new TupleDesc();
        schema.addField(Type.INT, "id");
        schema.addField(Type.STRING, "name");
        schema.addField(Type.INT, "age");

        // Initialize Storage
        HeapFile heapFile = new HeapFile(dbFile);
        BufferManager bm = new BufferManager(heapFile, 50);

        HeapPage heapPage;
        if (isExistingDb) {
            Page p0 = heapFile.readPage(0);
            heapPage = new HeapPage(p0);
        } else {
            Page p0 = new Page(0);
            heapPage = new HeapPage(p0);
        }

        // Initialize Index
        HeapFile indexDisk = new HeapFile(indexFile);
        if (!isExistingDb || !indexFile.exists()) {
            Page rootPage = new Page(0);
            indexDisk.writePage(rootPage);
        }
        BufferManager indexBm = new BufferManager(indexDisk, 50);
        BTreeFile index = new BTreeFile(indexBm, 0);

        // 2. Launch GUI
        // We pass ALL the necessary backend objects to the GUI
        SwingUtilities.invokeLater(() -> {
            new DBGui(heapPage, index, heapFile, indexBm, schema).show();
        });
    }
}