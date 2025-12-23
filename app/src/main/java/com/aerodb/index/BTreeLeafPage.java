package com.aerodb.index;
import com.aerodb.storage.Page;
import com.aerodb.storage.RecordId;
public class BTreeLeafPage extends BTreePage {
    
    //KEY(4 BYTE)+PAGEID(4 BYTES)+SLOTID(4 BYTES)
    private static final int ENTRY_SIZE=12;
    private static final int PAGE_SIZE=4096;
    public BTreeLeafPage(Page page){
        //Calculating max capacity based on page size 
        super(page,TYPE_LEAF,(PAGE_SIZE-HEADER_SIZE)/ENTRY_SIZE);

    }


    //Function for insertion
    public void insert(int key,RecordId rid){
        int count=getKeyCount();
        if(count>=getMaxCapacity()){
            throw new IllegalStateException("Leaf Page is full, cannot insert new key");
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
        setValueAt(targetIndex,rid);
        //3.update the count value

        setKeyCount(count+1);
    }

    //hepler function
    public RecordId lookup(int key){
        for(int i=0;i<getKeyCount();i++){
            if( getKeyAt(i)==key){
                return getValueAt(i);
            }
        }
        return null;//key not found
    }

    private int getKeyAt(int index){
        int offset=HEADER_SIZE+index*ENTRY_SIZE;
        return page.getInt(offset);
    }

    private void setKeyAt(int index,int key){
        int offset=HEADER_SIZE+index*ENTRY_SIZE;
        page.setInt(offset,key);
    }

    private RecordId getValueAt(int index){
        int offset=HEADER_SIZE+index*ENTRY_SIZE+4;
        int pageId=page.getInt(offset);
        int slotNumber=page.getInt(offset+4);
        return new RecordId(pageId,slotNumber);
    }

    private void setValueAt(int index,RecordId rid){
        int offset=HEADER_SIZE+index*ENTRY_SIZE+4;
        page.setInt(offset,rid.pageId);
        page.setInt(offset+4,rid.slotNumber);
    }

    private void copyEntry(int fromIndex,int toIndex){
        setKeyAt(toIndex,getKeyAt(fromIndex));
        setValueAt(toIndex,getValueAt(fromIndex));
    }

    //function for slitting 

    public int split(BTreeLeafPage recipient){
        int count=getKeyCount();
        int splitIndex=count/2;
        int newCount=count - splitIndex;
        //MOve the enteries
        for(int i=0;i<newCount;i++){
            int srcIndex=splitIndex + i;
            recipient.setKeyAt(i, getKeyAt(srcIndex));
            recipient.setValueAt(i, getValueAt(srcIndex));
        }
        //Update counts
        recipient.setKeyCount(newCount);
        this.setKeyCount(splitIndex);

        return recipient.getKeyAt(0);//return the first key of the recipient
    }
   
    
}
