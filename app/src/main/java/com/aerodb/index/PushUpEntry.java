package com.aerodb.index;

public class PushUpEntry {
    public int key;
    public int childPageId;
    //constructor
    public PushUpEntry(int key,int childPageId)
    {
        this.key=key;
        this.childPageId=childPageId;
    }
}
