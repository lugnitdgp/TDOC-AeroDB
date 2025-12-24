package com.aerodb.index;

import com.aerodb.buffer.*;
import com.aerodb.storage.Page;
import com.aerodb.storage.RecordId;

public class BTreeFile {
    private final BufferManager bufferManager;
    private int rootPageId;

    public BTreeFile(BufferManager bufferManager, int rootPageId){

        this.bufferManager=bufferManager;
        this.rootPageId=rootPageId;
        initRootIfNeeded();
    }

    private void initRootIfNeeded(){
        //Initialize empty root if needed
        try {
            Page p=bufferManager.getPage(rootPageId);
            if(p.getInt(0)==0 && p.getInt(4)==0){
                BTreeLeafPage leaf = new BTreeLeafPage(p);
                leaf.setPageType(BTreePage.TYPE_LEAF);
                bufferManager.setPageDirty(rootPageId, true);
            }
        } catch (Exception e) {}
    }

    public RecordId find(int key){
        int currentPageId =rootPageId;

        while (true) {
            Page rawPage =bufferManager.getPage(currentPageId);
            int pageType=rawPage.getInt(0);

            if (pageType==BTreePage.TYPE_LEAF) {
                return new BTreeLeafPage(rawPage).lookup(key);
            }
            else
                currentPageId =new BTreeInternalPage(rawPage).lookup(key);
        }
    }

    public void insert (int key, RecordId rid){
        //recursiveInsert returns a PushUpEntry if the root splits
        PushUpEntry result=insertRecursive(rootPageId, key, rid);

        if(result!=null){
            //Root Split
            createNewRoot(result);
        }
    }

    //Recursive Helper. Returns PushUpEntry if the child slits, null otherwise
    private PushUpEntry insertRecursive(int currentPageId, int key, RecordId rid){
        Page rawPage=bufferManager.getPage(currentPageId);
        int pageType=rawPage.getInt(0);

        if(pageType==BTreePage.TYPE_LEAF){
            return handleLeafInsert(rawPage, key, rid);
        }
        else{
            return handleInternalInsert(rawPage, key, rid);
        }
    }

    private PushUpEntry handleLeafInsert(Page rawPage, int key, RecordId rid){
        BTreeLeafPage leaf=new BTreeLeafPage(rawPage);

        //Case 1: Leaf has space. Just insert
        if(leaf.getKeyCount()<leaf.getMaxCapacity()){
            leaf.insert(key, rid);
            bufferManager.setPageDirty(leaf.page.getPageId(), true);
            return null;
        }

        //Case 2: Leaf is full. SPLIT
        //A. Allocate new page
        int newPageId=bufferManager.allocateNewPage();
        Page newRawPage=bufferManager.getPage(newPageId);
        BTreeLeafPage newLeaf =new BTreeLeafPage(newRawPage);

        //B. Insert the new key into the correct page (Old or New) temp?
        //Simplified: We split first, then insert the new key into the correct half.
        //Note: For Simplicity in this tutorial, we assume the new key fits after split

        int splitKey=leaf.split(newLeaf);

        //Decide where to put the new value
        if(key>=splitKey)
            newLeaf.insert(key,rid);
        else
            leaf.insert(key, rid);
        bufferManager.setPageDirty(leaf.page.getPageId(), true);
        bufferManager.setPageDirty(newLeaf.page.getPageId(), true);

        //C. Return the notification to the parent
        return new  PushUpEntry(splitKey, newPageId);
    }

    private PushUpEntry handleInternalInsert(Page rawPage, int key, RecordId rid){
        BTreeInternalPage internal = new BTreeInternalPage(rawPage);
        int childPageId=internal.lookup(key);

        //Recursively go down
        PushUpEntry result = insertRecursive(childPageId, key, rid);

        //If  child didn't split, we are done
        if(result==null) return null;

        //If child DID SPLIT we must insert the pushedUp key into this internal node
        if(internal.getKeyCount()<internal.getMaxCapacity()){
            internal.insert(result.key, result.childPageId);
            bufferManager.setPageDirty(internal.page.getPageId(), true);
            return null;
        }

        else{
            // THis node is also full. So SPLIT it also
            //For now, lets throw exception to keep this step digestable.
            //(Inplementing Internal Split is 80% similar to Leaf Split)
            throw new RuntimeException("Internal node full-> Depth >2 not implemented yet");
        }
    }

    private void createNewRoot(PushUpEntry result){
        //1. Allocate a new page for the new root
        int newRootId =bufferManager.allocateNewPage();
        Page newRootRaw= bufferManager.getPage(newRootId);
        BTreeInternalPage newRoot=new BTreeInternalPage(newRootRaw);

        //2. Point the new root to the Old Root (Left) and new child (Right)
        newRoot.setPointer(0, rootPageId); //Old Root becomes left child
        newRoot.insert(result.key, result.childPageId); //PushUp key points to right child

        //3. Update the global root pointer
        this.rootPageId=newRootId;
        bufferManager.setPageDirty(newRootId, true);

        //NOTE: In a Real DB, you must update the "Header Page" on disk to save the new Root ID
        System.out.println("The Tree Grew in Height");

    }

    



   
}
