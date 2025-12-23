package com.aerodb.index;

import com.aerodb.storage.Page;
public class BTreePage {
    
    protected Page page;
    //offsets for the header
    private static final int OFFSET_TYPE=0;
    private static final int OFFSET_COUNT=4;
    private static final int OFFSET_MAX=8;
    protected static final int HEADER_SIZE=12;//byte used by header

    public static final int TYPE_INTERNAL=0;
    public static final int TYPE_LEAF=1;

    public BTreePage(Page page,int type,int maxCapacity){
        this.page=page;
        setPageType(type);
        setMaxCapacity(maxCapacity);
        setKeyCount(0);
    }

    public int getPageType(){
        return page.getInt(OFFSET_TYPE);
    }
    public void setPageType(int type){
        page.setInt(OFFSET_TYPE,type);
    }

    public int getKeyCount(){
        return page.getInt(OFFSET_COUNT);
    }
    public void setKeyCount(int count){
        page.setInt(OFFSET_COUNT,count);
    }
    public int getMaxCapacity(){
        return page.getInt(OFFSET_COUNT);
    }
    public void setMaxCapacity(int max){
        page.setInt(OFFSET_MAX,max);
    }

    public boolean isLeaf(){
        return getPageType() == 1;
    }

}

