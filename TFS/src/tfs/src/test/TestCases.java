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
        testClient = new Client(args[0]);
        try {
            testClient.ConnectToServer();
            TestCases tc = new TestCases();

            System.out.println("Type a test with valid parameters and press enter: ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String input = "";
            while (true) {
                input = br.readLine();
                String[] params = input.split(" ");
                switch (params[0].toLowerCase()) {
                    case "test1":
                    case "unit1":
                        int param = Integer.parseInt(params[1]);
                        int param2 = Integer.parseInt(params[2]);
                        tc.test1(param, param2);
                        break;
                    case "test2":
                    case "unit2":
                        param = Integer.parseInt(params[2]);
                        tc.test2(params[1], param);
                        break;
                    case "test3":
                    case "unit3":
                        tc.test3(params[1]);
                        break;
                    case "test4":
                    case "unit4":
                        tc.test4(params[1], params[2], Integer.valueOf(params[3]));
                        break;
                    case "test5":
                    case "unit5":
                        tc.test5(params[1], params[2]);
                        break;
                    case "test6":
                    case "unit6":
                        tc.test6(params[1], params[2]);
                        break;
                    case "test7":
                    case "unit7":
                        tc.test7(params[1]);
                        break;
                    case "test8":
                    case "unit8":
                        param = Integer.parseInt(params[1]);
                        tc.test8(args[0], params[2], params[3], param);
                        break;
                    default:
                        System.out.println("Invalid test case, please re-enter");
                        break;
                }
                while(!br.ready()) {
                    testClient.RunLoop();
                }
            }
        } catch (IOException ie) {
            System.out.println(ie.getMessage());
        } catch (InterruptedException ie) {
            System.out.println(ie.getMessage());
        }
    }

    /**
     * Create a hierarchical directory structure.
     */
    public void test1(int numDirs, int numSubs) {
        try {
            if (numSubs == 0) {
                for (int i = 1; i <= numDirs; i++) {
                    testClient.CreateDir(Integer.toString(i));
                    //System.out.println(i);
                }
                return;
            } else {
                List<String> dirs = new ArrayList();
                dirs.add("1");

                int index = 0;
                int num = 2;
                while (dirs.size() < numDirs) {
                    for (int i = 0; i < numSubs; i++) {
                        if (dirs.size() == numDirs) {
                            break;
                        }

                        dirs.add(dirs.get(index) + "/" + num);
                        num++;
                    }
                    index++;
                }
                for (int i = 0; i < dirs.size(); i++) {
                    testClient.CreateDir(dirs.get(i));
                    //System.out.println(dirs.get(i));
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void test2(String filePath, int numFiles) throws InterruptedException{

        try {
            testClient.GetListFile(filePath);
            String[] paths = testClient.GetListFile();
            for (String path : paths) {

                testClient.ListFile(path);
                for (int i = 1; i <= numFiles; i++) {
                    String fileName = path + "/File" + Integer.toString(i);
                    testClient.CreateFile(fileName);
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void test3(String pathName) {
        try {
            testClient.DeleteFile(pathName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void test4(String localpath, String remotepath, int numReplicas) {
        try {
            testClient.WriteFile(localpath, remotepath, numReplicas);

        } catch (IOException e) {
            System.out.println("Test 4 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void test5(String pathname, String localpath) {
        try {
            testClient.ReadFile(pathname, localpath);
        } catch (IOException e) {
            System.out.println("Test 5 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void test6(String localpath, String pathname) {
        try {
            testClient.AppendFile(localpath, pathname);
        } catch (IOException e) {
            System.out.println("Test 6 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void test7(String pathname) {
        try {
            testClient.CountFiles(pathname);
        } catch (IOException e) {
            System.out.println("Test 6 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void test8(String port, String localpath, String pathname, int numInstances) {
        try {
            Client n[] = new Client[numInstances];
            for (int i = 1; i <= numInstances; i++) {
                n[i] = new Client(port);
                n[i].AppendFile(localpath, pathname);

            }
        } catch (IOException e) {
            System.out.println("Test 8 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

}
