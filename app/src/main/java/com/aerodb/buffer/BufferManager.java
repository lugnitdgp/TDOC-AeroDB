package com.aerodb.buffer;

import com.aerodb.storage.Page;
import com.aerodb.storage.HeapFile;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class BufferManager {
    
    private final HeapFile diskManager;
    private final Map<Integer, Page> pageCache;
    private final int maxPages;

    private final Map<Integer, Boolean> dirtyPages;

    public BufferManager (HeapFile diskManager, int maxPages){

        //Store dependencies
        this.diskManager=diskManager;
        this.maxPages=maxPages;
        this.dirtyPages=new HashMap<>();

        //The LRU Cache
        // Initial Capacity: maxPages
        // Load Factor: 0.75f (standard)
        // Access Order: true (Crucial for LRU: orders by access, not insertion)
        this.pageCache=new LinkedHashMap<>(maxPages,0.75f,true){
            /**
             * This method is called by LinkedHashMap after every 'put' or 'putAll'.
             * If the map size exceeds the limit, the eldest (least recently used) entry is returned here.
             */
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer,Page> eldest){
                if(size()>BufferManager.this.maxPages){
                    evictPage(eldest.getKey());
                    return true;
                }
                return false;
            }
        };
    }

    //Retrives a Page. If not in cache, loads it from disk.
    public Page getPage(int pageId){
        if (pageCache.containsKey(pageId))
            return pageCache.get(pageId);

        //Not in cache? Load it from Disk
        Page p =diskManager.readPage(pageId);
        pageCache.put(pageId,p);
        return p;
    }

    /**
     * Marks a page as dirty (Modified)
     * It will be written to disk when evicted or flushed
     */
    public void setPageDirty(int pageId, boolean dirty){
        if (pageCache.containsKey(pageId))
            dirtyPages.put((pageId), dirty);
    }

    //Helper to write a specific page back to disk
    private void evictPage(int pageId){
        if(dirtyPages.getOrDefault(pageId, false)){
            Page p= pageCache.get(pageId);
            if(p!=null){
                diskManager.writePage(p);
                dirtyPages.remove(p);
            }
        }
    }

    //Forces all dirty pages to disk
    /**
     * Flushes all pages in the cache.
     * Iterates through every page in memory, writes dirty ones to disk,
     * and completely clears the cache.
     * * Typically used during system shutdown or transaction commits.
     */
    public void flushAll(){
        for(Integer pageId: pageCache.keySet()){
            evictPage(pageId);
        }
        pageCache.clear();
    }

    public int allocateNewPage(){
        //Calculate the newID baised on current file size
        int newPageId=diskManager.getNumPages();
        //Just reading it will create it in our specific HeapFile implementation logic
        //But explicitly we must create a blank page
        Page p= new Page(newPageId);
        diskManager.writePage(p);
        return newPageId;
    }


}
