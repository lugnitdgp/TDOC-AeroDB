package com.aerodb.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import javax.management.RuntimeMBeanException;

public class HeapFile {
    private File file;
    private RandomAccessFile raf;


    //constructor
    public HeapFile(File f){
        this.file=f;
        try{

            this.raf=new RandomAccessFile(f,"rw");
        }catch(FileNotFoundException e){
            throw new RuntimeException("Could not open DB file"+f.getPath(),e);
        }

    }
    //Reads page function to read specific p[age from the disk
    public Page readPage(int pageId){
        Page p=new Page(pageId);
        int offset=pageId*Page.PAGE_SIZE;
        try{
            if(offset+Page.PAGE_SIZE>raf.length()){
                throw new IllegalArgumentException("Page "+pageId+" does not exit in the file");
            }
            raf.seek(offset);
            raf.readFully(p.getData());
            return p;
        }catch(IOException e){
            throw new RuntimeException("Error reading a page "+pageId,e);
        }
    }
    //Writing a Page function
    public void writePage(Page p){
        int offset=p.getPageId()*Page.PAGE_SIZE;
        try{
            raf.seek(offset);
            raf.write(p.getData());
        }catch(IOException e){
            throw new RuntimeException("Error writing page"+p.getPageId(),e);
        }
    }

    //return the number of pages currently in the file
    public int getNumPages(){
        try{
            return (int)(raf.length()/Page.PAGE_SIZE);
        }
        catch(IOException e){
            throw new RuntimeException("Errr getting file size",e);
        }
    }

    //for closing the raf stream
    public void close() throws IOException{
        raf.close();
    }

    
}
