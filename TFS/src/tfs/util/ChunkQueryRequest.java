/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.util;

import java.io.*;
import java.util.*;

/**
 *
 * @author laurencewong
 */
public class ChunkQueryRequest {
    
    public enum QueryType {
        APPEND,
        READ
    }
    
    String mChunkName;
    byte[] mData; //either data to be written or data to be read
    QueryType mQueryType;
    
    public ChunkQueryRequest(String chunkName, QueryType type) {
        mChunkName = chunkName;
        mData = new byte[0];
        mQueryType = type;
    }
    
    public void PutData(byte[] inData) {
        mData = inData;
    }
    
    public byte[] GetData() {
        return mData;
    }
    
    public String GetName() {
        return mChunkName;
    }
    
}
