package com.aerodb.index;


import com.aerodb.storage.Page;

public class BTreeInternalPage extends BTreePage{

 private static final int ENTRY_SIZE=8;
 
 public BTreeInternalPage(Page page){
    //Calculating max capacity based on page size
    super(page,TYPE_INTERNAL,(4096-HEADER_SIZE)/ENTRY_SIZE);
 }
 //Function for lookup
    public int lookup(int key){
        int count=getKeyCount();
        for(int i=count-1;i>0;i--){
            if(key>=getKeyAt(i)){
                return getValueAt(i);
            }
        }

        return getValueAt(0);//return first child pointer
    
}

//Function for Insertion 
public void insert(int key,int childPageId){
    int count=getKeyCount();
    if(count>=getMaxCapacity()){
        throw new IllegalStateException("Internal Page is full, cannot insert new key");
    }

    //1.Find the position to insert
    int i=count-1;
    while(i>=0 && getKeyAt(i)>key){
        //shift right
        copyEntry(i,i+1);
        i--;
    }

    //2.Insert the new Entry
    int targetIndex=i+1;
    setKeyAt(targetIndex,key);
    setValueAt(targetIndex,childPageId);
    //3.update the count value

    setKeyCount(count+1);
}


public void setPointer(int index,int childPageId){
    setValueAt(index,childPageId);
}


//helper function for the above operations
 private int getKeyAt(int index){
    int offset=HEADER_SIZE+index*ENTRY_SIZE;
    return page.getInt(offset);
 }

 private void setKeyAt(int index,int key){
    int offset=HEADER_SIZE+index*ENTRY_SIZE;
    page.setInt(offset,key);
 }

 private int getValueAt(int index){
    int offset=HEADER_SIZE+(index*ENTRY_SIZE)+4; //skip the key part
    return page.getInt(offset);
 }

 public void setValueAt(int index,int ChildPageId){
    int offset=HEADER_SIZE+(index*ENTRY_SIZE)+4;
    page.setInt(offset,ChildPageId);
 }

 private void copyEntry(int fromIndex,int toIndex){
    setKeyAt(toIndex,getKeyAt(fromIndex));
    setValueAt(toIndex,getValueAt(fromIndex));
 }
}