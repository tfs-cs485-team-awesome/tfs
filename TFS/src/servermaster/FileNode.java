/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package servermaster;

import java.util.ArrayList;

/**
 *
 * @author laurencewong
 */
public class FileNode {
    
    public FileNode(boolean isFile) {
        mIsDirectory = !isFile;
        if(!isFile){
            mChildren = new ArrayList<FileNode>();
        }
        if(isFile) {
            mFileMetadata = new FileMetadata();
        }
    }
    /**
    * Metadata for each chunk
    * Holds the location of that chunk and the locations of each of the replicas
    * for that chunk
    */
    private class ChunkMetadata {
        String mLocation;
        ArrayList<String> mReplicaLocations;

        public ChunkMetadata() {
                mReplicaLocations = new ArrayList<String>();
        }
    }
    
    /**
     * Metadata for each file
     * Holds the list of chunks that make up a file
     */
    private class FileMetadata {
        ArrayList<ChunkMetadata> mChunks;

        public FileMetadata() {
                mChunks = new ArrayList<ChunkMetadata>();
        }
    }
    String mName;
    boolean mIsDirectory;
    
    //false = unlocked
    //true  = locked
    boolean mReadLock, mWriteLock;
    ArrayList<FileNode> mChildren;

    //only need to make this if this is actually a file
    FileMetadata mFileMetadata;

}
