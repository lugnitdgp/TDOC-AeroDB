package com.aerodb.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages a Page as a collection of Tuples using Slotted Page architecture.
 * Layout:
 * [Header: Count(4B), FreeSpacePtr(4B)]
 * [Slot 0: Offset(4B), Length(4B)]
 * [Slot 1: Offset(4B), Length(4B)]
 * ...
 * [Free Space]
 * ...
 * [Tuple Data 1]
 * [Tuple Data 0] (Data grows backwards from the end)
 */
public class HeapPage {
    private Page page;
    
    // Header layout
    private static final int OFF_COUNT = 0;
    private static final int OFF_FREE_PTR = 4;
    private static final int HEADER_SIZE = 8;
    
    // Each slot is 8 bytes: [Offset (4), Length (4)]
    private static final int SLOT_SIZE = 8; 

    public HeapPage(Page page) {
        this.page = page;
        // Initialize new page: Free Pointer starts at the very end
        if (getNumTuples() == 0 && getFreeSpacePtr() == 0) {
            setFreeSpacePtr(Page.PAGE_SIZE);
        }
    }

    public int getNumTuples() {
        return page.getInt(OFF_COUNT);
    }

    private void setNumTuples(int count) {
        page.setInt(OFF_COUNT, count);
    }

    private int getFreeSpacePtr() {
        return page.getInt(OFF_FREE_PTR);
    }

    private void setFreeSpacePtr(int offset) {
        page.setInt(OFF_FREE_PTR, offset);
    }

    /**
     * Inserts a Tuple into this page.
     * Returns the slot number where it was stored.
     */
    public int insertTuple(Tuple t) {
        byte[] data = t.serialize();
        int dataLen = data.length;
        
        // 1. Check if we have enough space
        // Space needed = Data Length + Slot Entry Size
        int slotsEnd = HEADER_SIZE + (getNumTuples() * SLOT_SIZE);
        int freeSpace = getFreeSpacePtr() - slotsEnd;

        if (freeSpace < dataLen + SLOT_SIZE) {
            throw new RuntimeException("Page is Full! implementation needed: Create new Page");
        }

        // 2. Write Data (Backwards from the current FreeSpacePtr)
        int writeStart = getFreeSpacePtr() - dataLen;
        System.arraycopy(data, 0, page.getData(), writeStart, dataLen);
        setFreeSpacePtr(writeStart);

        // 3. Write Slot (Forwards from the last slot)
        int slotIdx = getNumTuples();
        setSlot(slotIdx, writeStart, dataLen);

        // 4. Update Header
        setNumTuples(slotIdx + 1);
        
        return slotIdx;
    }

    /**
     * Reads a Tuple from a specific slot.
     */
    public Tuple getTuple(int slotId, TupleDesc td) {
        if (slotId >= getNumTuples()) {
            throw new IllegalArgumentException("Invalid slot: " + slotId);
        }

        int offset = getSlotOffset(slotId);
        int len = getSlotLength(slotId);

        byte[] data = new byte[len];
        System.arraycopy(page.getData(), offset, data, 0, len);

        Tuple t = new Tuple(td);
        t.deserialize(data);
        return t;
    }
    
    /**
     * Helper to retrieve all valid tuples in this page.
     */
    public List<Tuple> getAllTuples(TupleDesc td) {
        List<Tuple> tuples = new ArrayList<>();
        for (int i = 0; i < getNumTuples(); i++) {
            tuples.add(getTuple(i, td));
        }
        return tuples;
    }

    // --- Slot Helpers ---

    private void setSlot(int index, int offset, int length) {
        int slotPos = HEADER_SIZE + (index * SLOT_SIZE);
        page.setInt(slotPos, offset);
        page.setInt(slotPos + 4, length);
    }

    private int getSlotOffset(int index) {
        int slotPos = HEADER_SIZE + (index * SLOT_SIZE);
        return page.getInt(slotPos);
    }

    private int getSlotLength(int index) {
        int slotPos = HEADER_SIZE + (index * SLOT_SIZE);
        return page.getInt(slotPos + 4);
    }
    
    public Page getPage() {
        return this.page;
    }
}