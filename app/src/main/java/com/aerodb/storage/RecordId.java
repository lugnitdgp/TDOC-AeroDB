package com.aerodb.storage;

public class RecordId {

    public int pageId;
    public int slotNumber;
    //constructor
    public RecordId(int pageId,int slotNumber){
        this.pageId=pageId;
        this.slotNumber=slotNumber;
    }
}
