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

    public Client testClient;
    public String ipAndPort;

    public static void main(String[] args) {
        TestCases tc = new TestCases();

        tc.testClient = new Client(args[0]);
        tc.ipAndPort = args[0];
        try {
            tc.testClient.ConnectToServer();

            System.out.println("Type a test with valid parameters and press enter: ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String input = "";
            while (true) {
                input = br.readLine();
                String[] params = input.split(" ");
                tc.EvaluateInput(params);
                /*switch (params[0].toLowerCase()) {
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
                    case "bunchofclients":
                        param = Integer.parseInt(params[1]);
                        break;
                    default:
                        System.out.println("Invalid test case, please re-enter");
                        break;
                }
                */
                while (!br.ready()) {
                    tc.testClient.RunLoop();
                }
            }
        } catch (IOException ie) {
            System.out.println(ie.getMessage());
        } catch (InterruptedException ie) {
            System.out.println(ie.getMessage());
        }
    }

    public void EvaluateInput(String[] params) {
        try {
            switch (params[0].toLowerCase()) {
                case "test1":
                case "unit1":
                    int param = Integer.parseInt(params[1]);
                    int param2 = Integer.parseInt(params[2]);
                    test1(param, param2);
                    break;
                case "test2":
                case "unit2":
                    param = Integer.parseInt(params[2]);
                    test2(params[1], param);
                    break;
                case "test3":
                case "unit3":
                    test3(params[1]);
                    break;
                case "test4":
                case "unit4":
                    test4(params[1], params[2], Integer.valueOf(params[3]));
                    break;
                case "test5":
                case "unit5":
                    test5(params[1], params[2]);
                    break;
                case "test6":
                case "unit6":
                    test6(params[1], params[2]);
                    break;
                case "test7":
                case "unit7":
                    test7(params[1]);
                    break;
                case "test8":
                case "unit8":
                    param = Integer.parseInt(params[1]);
                    test8(ipAndPort, params[2], params[3], param);
                    break;
                case "bunchofclients":
                    param = Integer.parseInt(params[1]);
                    String[] newParams = new String[params.length - 2];
                    System.arraycopy(params, 2, newParams, 0, params.length - 2);
                    break;
                default:
                    System.out.println("Invalid test case, please re-enter");
                    break;
            }
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

    public void test2(String filePath, int numFiles) throws InterruptedException {

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
            System.out.println("Test 7 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    private class ClientTimerTask extends Thread {

        Client c;
        boolean isRunning = true;
        Timer stopTimer;

        public ClientTimerTask(String ipAndPort, String inlocal, String inpath) {
            System.out.println("Starting new client");
            c = new Client(ipAndPort);
            stopTimer = new Timer();
            stopTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    stopRunning();
                }
            }, 1000);
            try {
                c.ConnectToServer();

                c.AppendFile(inlocal, inpath);
                this.start();
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
        }

        public void stopRunning() {
            isRunning ^= true;
        }

        @Override
        public void run() {
            try {
                while (isRunning) {

                    c.RunLoop();
                }
                System.out.println("Exiting client");
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            } catch (InterruptedException ie) {
                System.out.println(ie.getMessage());
            }
        }

    }

    public void test8(final String ipAndPort, final String localpath, final String pathname, int numInstances) {
        Random r = new Random();
        Timer clientTimer = new Timer();
        for (int i = 1; i <= numInstances; i++) {
            /*n[i] = new Client(ipAndPort);
             n[i].AppendFile(localpath, pathname);*/
            clientTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    new ClientTimerTask(ipAndPort, localpath, pathname);
                }
            }, 100 + r.nextInt(300));
        }
    }

}
