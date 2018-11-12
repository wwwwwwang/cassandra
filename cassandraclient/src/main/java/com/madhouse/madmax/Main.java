package com.madhouse.madmax;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.madhouse.madmax.utils.CassandraClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wujunfeng on 2018-03-30.
 */
public class Main {
    private static ArrayList<String> getByteArrayFromFile(String path) {
        File readFile = new File(path);
        InputStream in = null;
        InputStreamReader ir = null;
        BufferedReader br = null;
        ArrayList<String> res = new ArrayList<String>();

        try {
            in = new BufferedInputStream(new FileInputStream(readFile));
            ir = new InputStreamReader(in, "utf-8");
            br = new BufferedReader(ir);
            String line;
            int cnt = 0;
            while ((line = br.readLine()) != null) {
                if (line.length() > 3) {
                    String rowkey = line.toLowerCase();
                    cnt++;
                    res.add(rowkey);
                }
            }
            System.out.println("there are " + cnt + " records in the file:" + path);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (ir != null) {
                    ir.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (Exception ignored) {
            }
        }
        return res;
    }

    public static void main(String[] args) {
        Options opt = new Options();
        opt.addOption("s", "seed", true, "seeds of cassandra");
        opt.addOption("c", "columnfamilies", true, "table name in cassandra");
        opt.addOption("k", "keyspace", true, "keyspace name in cassandra");
        opt.addOption("h", "help", false, "help message");
        opt.addOption("p", "path", true, "the file name used to make the path of input file");
        opt.addOption("t", "thread", true, "the count of threads, default: 10");
        opt.addOption("m", "monitor", false, "whether monitor the cassandra request status");

        int threadCount = 10;
        String path = "20";
        String seeds = "127.0.0.1";
        String keyspace = "w";
        String table = "tt";
        boolean monitor = false;

        String formatstr = "java -jar cassandraclient-1.0-SNAPSHOT-jar-with-dependencies.jar ...";
        HelpFormatter formatter = new HelpFormatter();
        PosixParser parser = new PosixParser();

        CommandLine cl = null;
        try {
            cl = parser.parse(opt, args);
        } catch (Exception e) {
            e.printStackTrace();
            formatter.printHelp(formatstr, opt);
            System.exit(1);
        }

        if (cl.hasOption("s")) {
            seeds = cl.getOptionValue("s");
        }
        if (cl.hasOption("k")) {
            keyspace = cl.getOptionValue("k");
        }
        if (cl.hasOption("c")) {
            table = cl.getOptionValue("c");
        }
        if (cl.hasOption("h")) {
            formatter.printHelp(formatstr, opt);
            System.exit(0);
        }
        if (cl.hasOption("p")) {
            path = cl.getOptionValue("p");
        }
        if (cl.hasOption("t")) {
            threadCount = Integer.parseInt(cl.getOptionValue("t"));
        }
        if (cl.hasOption("m")) {
            monitor = true;
        }


        System.out.println("##### seeds=" + seeds +
                ", keyspace = " + keyspace +
                ", table = " + table +
                ", path = " + path +
                ", threadcount = " + threadCount +
                ", monitor = " + monitor);


        final CassandraClient client = new CassandraClient(seeds.split(","), keyspace, table);

        System.out.println("###start..");
        Long s = System.currentTimeMillis();
        String filePath = "/home/wanghaishen/apps/cassandraclient/id" + path + ".csv";
        final ArrayList<String> ids = getByteArrayFromFile(filePath);
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < ids.size(); i++) {
            final int finalI = i;
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        String did = ids.get(finalI);
                        if (did.contains("-")) {
                            did = did.toUpperCase();
                        }
                        long st = System.currentTimeMillis();
                        ResultSet results = client.query(did);
                        if (results != null) {
                            /*for (Row row : results) {
                                System.out.println("#####tags size = " + row.getSet("tag", String.class).size());
                            }*/
                            System.out.println("#####get records by did list finishes with time costing " + (System.currentTimeMillis() - st) + "ms");
                        } else {
                            System.out.println("#####failed: " + did);
                        }
                        //Thread.sleep(500);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            exec.execute(run);
        }
        exec.shutdown();
        if(monitor){
            client.monitor();
        }
        try {
            while (!exec.isTerminated()) {
                Thread.sleep(200);
                //System.out.println("Executors is not terminated!");
            }
            System.out.println("#####end...");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Long e = System.currentTimeMillis();
        System.out.println("##### time cost = " + (e - s) / 1000 + "s!");
        client.close();
        System.exit(0);
    }
}

