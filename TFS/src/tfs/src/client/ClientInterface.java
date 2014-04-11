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
    public String[] GetListFile(String path) throws IOException;
    public byte[] ReadFile(String fileName) throws IOException;
    public FileNode GetAtFilePath(String path) throws IOException;
    public void WriteFile(String fileName) throws IOException;
    public void WriteLocalFile(String fileName, byte[] data) throws IOException;
}
