/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.src.client;


import java.io.*;
import tfs.util.FileNode;
/**
 *
 * @author laurencewong
 */
public interface ClientInterface {
    public void CreateFile(String fileName) throws IOException;
    public void CreateDir(String dirName) throws IOException;
    public void DeleteFile(String fileName) throws IOException;
    public void ListFile(String path) throws IOException;
    public void GetListFile(String path) throws IOException;
    public String[] GetListFile() throws IOException, InterruptedException;
    public void ReadFile(String remotefilename, String localfilename) throws IOException;
    public void GetAtFilePath(String path) throws IOException;
    public FileNode GetAtFilePath() throws IOException, InterruptedException;
    public void WriteFile(String localfilename, String remotefilename, int numReplicas) throws IOException;
    public void AppendFile(String localfilename, String remotefilename) throws IOException;
    public void CountFiles(String remotename) throws IOException;
    public void WriteLocalFile(String fileName, byte[] data) throws IOException;
}
