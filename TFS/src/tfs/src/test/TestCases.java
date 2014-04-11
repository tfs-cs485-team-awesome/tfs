/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.src.test;

import tfs.src.client.Client;
import tfs.util.FileNode;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import tfs.*;
import java.util.*;

/**
 *
 * @author Kevin
 */
public class TestCases {
    static Client testClient;
    public static void main(String[] args) {
        testClient = new Client("localhost:6789");
        try {
            testClient.ConnectToServer();
            TestCases tc = new TestCases();
            
            tc.test1(25);
            tc.test2("/1/2", 5);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
        /**
     * Create a hierarchical directory structure.   
     */
    public void test1 (int dirs) {
        
        try {
            int i = 2;
            Queue q = new LinkedList();
            q.add("1");
            testClient.CreateDir("1");

            while (dirs - i >= 0) {
                String curstr = q.remove().toString();
                String temp = curstr + "/" + i;
                q.add(temp);
                testClient.CreateDir(temp);

                i++;
                if(dirs - i >= 0) {
                    temp = curstr + "/" + i;
                    q.add(temp);
                    testClient.CreateDir(temp);
                }
                i++;
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        
    }
    
    public void test2(String filePath, int numFiles) {
        
        try {
            String[] paths = testClient.GetListFile(filePath);
            /*
            List<String> toExplore = new ArrayList<>();
            List<String> explored = new ArrayList<>();

            for (String path : paths) {
                toExplore.add(path);
                //System.out.println("adding path " + path + " to toExplore");
            }

            while (toExplore != null) {
                    List<String> moreToExplore = new ArrayList<>();
                    for (String path : toExplore) {
                            String [] more = testClient.GetListFile(path);
                            for (String path2 : more) {
                                    moreToExplore.add(path2);
                            }
                            explored.add(path);
                    }
                    toExplore.clear();
                    toExplore = new ArrayList<String>(moreToExplore);
                    
            }
*/
            for (String path : paths) {
                testClient.ListFile(path);
                    for (int i = 1; i<= numFiles; i++) {
                            String fileName = path + "/File" + Integer.toString(i);
                            testClient.CreateFile(fileName);
                    }
            }  
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }
    
    public boolean test3(String pathname){
        try {
        testClient.DeleteFile(pathname);
        return true;
        }
        catch(IOException e) {
            System.out.println("Test 3 failed due to exception" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    public void test5(String pathname, String localpath) {
        try{
            byte[] data;
            data = testClient.SeekFile(pathname);
            if (data != null) {
                try{
                    testClient.WriteLocalFile(localpath, data);
                }
                catch(IOException e) {
                    System.out.println("Test 5 failed due to exception " + e.getMessage());
                    e.printStackTrace();
                }
                
            }
            else{
                System.out.println("Test 5 failed because the file requested does not exist.");
            }
        }
        catch(IOException e) {
            System.out.println("Test 5 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }


}
