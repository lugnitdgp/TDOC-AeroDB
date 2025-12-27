package com.aerodb;

import com.aerodb.buffer.BufferManager;
import com.aerodb.index.BTreeFile;
import com.aerodb.storage.*;

import java.io.File;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("=== JDB Storage Engine Starting ===");
        Scanner scanner = new Scanner(System.in);

        // 1. Setup Phase (Initialization)
        File dbFile = new File("data.db");
        File indexFile = new File("index.db");

        // Check if DB exists so we know whether to Load or Create
        boolean isExistingDb = dbFile.exists();

        // Define Schema: [ID (Int), Name (String), Age (Int)]
        TupleDesc schema = new TupleDesc();
        schema.addField(Type.INT, "id");
        schema.addField(Type.STRING, "name");
        schema.addField(Type.INT, "age");
        System.out.println("Schema Defined: [ID (int), NAME (string), AGE (int)]");

        // Initialize Storage Components
        HeapFile heapFile = new HeapFile(dbFile);
        BufferManager bm = new BufferManager(heapFile, 50); 
        
        // Initialize active Storage Page (Page 0)
        HeapPage heapPage;

        if (isExistingDb) {
            System.out.println(">> Found existing data. Loading Page 0...");
            // Load the existing page from disk
            Page p0 = heapFile.readPage(0);
            heapPage = new HeapPage(p0);
        } else {
            System.out.println(">> No existing data. Creating new Page 0...");
            // Create a fresh blank page
            Page p0 = new Page(0);
            heapPage = new HeapPage(p0);
        }
        
        // Initialize Index
        HeapFile indexDisk = new HeapFile(indexFile);
        
        // Only initialize the root page on disk if it's a NEW database
        // If it's an existing DB, we assume index.db has valid data from the flushAllPages call
        if (!isExistingDb || !indexFile.exists()) {
            Page rootPage = new Page(0);
            indexDisk.writePage(rootPage);
        }

        BufferManager indexBm = new BufferManager(indexDisk, 50);
        BTreeFile index = new BTreeFile(indexBm, 0); // Root at page 0

        System.out.println("Storage Engine Initialized. Ready for commands.");

        // 2. Interactive Loop
        boolean running = true;

        while (running) {
            System.out.println("\n--- MENU ---");
            System.out.println("1. INSERT Record");
            System.out.println("2. FIND Record (via BTree Index)");
            System.out.println("3. FLUSH & EXIT");
            System.out.print("Select option: ");

            int choice = -1;
            try {
                choice = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Invalid input.");
                continue;
            }

            switch (choice) {
                case 1: // INSERT
                    try {
                        System.out.print("Enter ID: ");
                        int id = Integer.parseInt(scanner.nextLine());
                        
                        System.out.print("Enter Name: ");
                        String name = scanner.nextLine();
                        
                        System.out.print("Enter Age: ");
                        int age = Integer.parseInt(scanner.nextLine());

                        // Create Tuple
                        Tuple t = new Tuple(schema);
                        t.setField(0, id);
                        t.setField(1, name);
                        t.setField(2, age);

                        int slot = heapPage.insertTuple(t);
                        
                        // Insert into Index
                        index.insert(id, new RecordId(0, slot));
                        
                        System.out.println("Success! Inserted [" + name + "] at Page 0, Slot " + slot);
                    } catch (Exception e) {
                        System.out.println("Error inserting data: " + e.getMessage());
                    }
                    break;

                case 2: // FIND
                    try {
                        System.out.print("Enter ID to search: ");
                        int searchKey = Integer.parseInt(scanner.nextLine());
                        
                        long startTime = System.nanoTime();
                        RecordId rid = index.find(searchKey);
                        long endTime = System.nanoTime();

                        if (rid != null) {
                            System.out.println("Index Hit! (Found in " + (endTime - startTime) + "ns)");
                            System.out.println("Location: Page " + rid.pageId + ", Slot " + rid.slotNumber);
                            
                            // Retrieve data
                            Tuple result;
                            if (rid.pageId == 0) {
                                result = heapPage.getTuple(rid.slotNumber, schema);
                            } else {
                                Page fetchedPage = heapFile.readPage(rid.pageId);
                                HeapPage fetchedHeapPage = new HeapPage(fetchedPage);
                                result = fetchedHeapPage.getTuple(rid.slotNumber, schema);
                            }
                            
                            System.out.println(">> RECORD: [ID: " + result.getField(0) 
                                             + " | Name: " + result.getField(1) 
                                             + " | Age: " + result.getField(2) + "]");
                        } else {
                            System.out.println(">> Result: Key " + searchKey + " not found.");
                        }
                    } catch (Exception e) {
                        System.out.println("Error searching data: " + e.getMessage());
                    }
                    break;

                case 3: // EXIT
                    System.out.println("Flushing data to disk...");
                    
                    // 1. Flush Data Page
                    heapFile.writePage(heapPage.getPage());
                    
                    // 2. Flush Index Pages (THIS WAS MISSING)
                    try {
                    
                        indexBm.flushAll(); 
                    } catch (Exception e) {
                        System.out.println("Warning: Error flushing index: " + e.getMessage());
                    }

                    System.out.println("Data and Index saved. Shutting down.");
                    running = false;
                    break;

                default:
                    System.out.println("Unknown command.");
            }
        }
        
        scanner.close();
    }
}