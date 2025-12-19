package com.aerodb.storage;

import java.nio.ByteBuffer;

public class Page {
    
    static final int PAGE_SIZE= 4096;
    private int pageId;
    private byte[] data;

    // Construtor for a new empty page
    public Page(int pageId){
        this.pageId=pageId;
        this.data=new byte[PAGE_SIZE]; //4kb zeros
    }

    //Constructor for loading existing data (from disk)
    public Page(int pageId, byte[]data){
        this.pageId=pageId;
        if(data.length!=PAGE_SIZE)
            throw new IllegalArgumentException("Data must be of 4096 bytes");
        this.data=data;
    }

    //Getters
    public int getID(){
        return pageId;
    }

    public byte[] getData(){
        return data;
    }

    /**    ByteBuffer function syntax
     * ByteBuffer.wrap(data).putInt(0,258)
     * int myId= ByteBuffer.wrap(data).getInt(5)
     * 
     * 500 in HEX 0x00 00 01 F4
     * 
     */

    //Helper to write an integer from a specific offset
    public void setInt(int offset, int value){
         //Use ByteBuffer to easily convert int to bytes
        ByteBuffer.wrap(data).putInt(offset,value);
    }

    //Helper to read an integer from a specific offset
    public int getInt(int offset){
        return ByteBuffer.wrap(data).getInt(offset);
    }

    @Override
    public String toString(){
        return "Page{id=" +pageId+ "}";
    }

    

}
