package org.voltdb.overspendflagger;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2024 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.stats.SafeHistogramCache;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 */
public class SimulateOverspends {

    public static final int HISTOGRAM_SIZE_MS = 1000000;

    public static final String DELETE_CAMPAIGN = "DELETE_CAMPAIGN";
    public static final String CREATE_CAMPAIGN = "CREATE_CAMPAIGN";
    public static final String CREATE_CAMPAIGN_ADS = "CREATE_CAMPAIGN_ADS";
    public static final String RUN_CAMPAIGN = "RUN_CAMPAIGN";

    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";
    public static final String EXTRA_MS = "EXTRA_MS";

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Connect to VoltDB using a comma delimited hostname list.
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    protected static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            config.setHeavyweight(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    protected static void deleteOldCampaigns(int tpMs, Client mainClient)
            throws InterruptedException, IOException, NoConnectionsException, ProcCallException {

        msg("Deleting old campaigns...");

        int campaignCount = getCampaignCount(mainClient);

        final long startMsDelete = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();

        // To make sure we do things at a consistent rate (tpMs) we
        // track how many transactions we've queued this ms and sleep if
        // we've reached our limit.
        int tpThisMs = 0;

        // So we iterate through all our users...
        for (int i = 0; i <= campaignCount; i++) {

            if (tpThisMs++ > tpMs) {

                // but sleep if we're moving too fast...
                while (currentMs == System.currentTimeMillis()) {
                    Thread.sleep(0, 50000);
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            // Put a request to delete a user into the queue.
            TimeTrackingCallback theCallback = new TimeTrackingCallback(shc, DELETE_CAMPAIGN);
            mainClient.callProcedure(theCallback, "delete_campaigns", i, i, i);

            if (i % 100000 == 1) {
                msg("Deleted " + i + " campaigns...");
            }

        }

        msg("Deleted " + campaignCount + " campaigns...");

        // Because we've put messages into the clients queue we
        // need to wait for them to be processed.
        msg("All " + campaignCount + " entries in queue, waiting for it to drain...");
        mainClient.drain();

        final long entriesPerMs = (campaignCount) / (System.currentTimeMillis() - startMsDelete);
        msg("Deleted " + entriesPerMs + " campaigns per ms...");
    }

    /**
     * @param mainClient
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private static int getCampaignCount(Client mainClient)
            throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr = mainClient.callProcedure("@AdHoc", "SELECT max(campaign_id) max_id FROM campaigns;");

        int campaignCount = 0;
        if (cr.getStatus() == ClientResponse.SUCCESS) {

            if (cr.getResults()[0].advanceRow()) {
                campaignCount = (int) cr.getResults()[0].getLong("max_id");

                if (cr.getResults()[0].wasNull()) {
                    campaignCount = 0;
                }

                msg("found " + campaignCount + " campaigns");
            }

        } else {
            msg("getCampaignCount failed: " + cr.getStatusString());
            System.exit(2);
        }
        return campaignCount;
    }

    protected static void createNewCampaigns(int campaignCount, int tpMs, Client mainClient, int budget, int adCount)
            throws InterruptedException, IOException, NoConnectionsException {

        msg("Creating new campaigns...");

        final long startMsDelete = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();
        Random r = new Random();

        // To make sure we do things at a consistent rate (tpMs) we
        // track how many transactions we've queued this ms and sleep if
        // we've reached our limit.
        int tpThisMs = 0;

        // So we iterate through all our users...
        for (int i = 0; i <= campaignCount; i++) {

            // Put a request to delete a user into the queue.
            TimeTrackingCallback theCallback = new TimeTrackingCallback(shc, CREATE_CAMPAIGN);
            mainClient.callProcedure(theCallback, "campaigns.INSERT", i, r.nextInt(budget));

            for (int j = 0; j < adCount; j++) {
                theCallback = new TimeTrackingCallback(shc, CREATE_CAMPAIGN_ADS);
                mainClient.callProcedure(theCallback, "campaign_ads_object.INSERT", i, j);

                if (tpThisMs++ > tpMs) {

                    // but sleep if we're moving too fast...
                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

            
            }

            if (i % 100000 == 1) {
                msg("Created " + i + " campaigns...");
            }

        }

        // Because we've put messages into the clients queue we
        // need to wait for them to be processed.
        msg("All " + campaignCount + " entries in queue, waiting for it to drain...");
        mainClient.drain();

        final long entriesPerMs = (campaignCount) / (System.currentTimeMillis() - startMsDelete);
        msg("Created " + entriesPerMs + " campaigns per ms...");
    }

    protected static void runOverspendBenchmark(int campaignCount, int tpMs, Client mainClient, int durationSeconds,
            int globalQueryFreqSeconds, int adCount)
            throws InterruptedException, IOException, NoConnectionsException, ProcCallException {

        msg("runOverspendBenchmark for " + durationSeconds + " seconds...");

        final long startMsBenchmark = System.currentTimeMillis();
        final long endMsBenchmark = startMsBenchmark + (1000 * durationSeconds);
        long currentMs = System.currentTimeMillis();
        long nextOverspendReportMs = System.currentTimeMillis() + (1000 * globalQueryFreqSeconds);
        Random r = new Random();
        long eventCount = 0;

        // To make sure we do things at a consistent rate (tpMs) we
        // track how many transactions we've queued this ms and sleep if
        // we've reached our limit.
        int tpThisMs = 0;

        // So we iterate through all our users...
        while (System.currentTimeMillis() <= endMsBenchmark) {

            if (tpThisMs++ > tpMs) {

                // but sleep if we're moving too fast...
                while (currentMs == System.currentTimeMillis()) {
                    Thread.sleep(0, 50000);
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            // Put a request to delete a user into the queue.
            TimeTrackingCallback theCallback = new TimeTrackingCallback(shc, RUN_CAMPAIGN);
            int campaignId = r.nextInt(campaignCount);
            int adId = r.nextInt(adCount);
            int clickCount = r.nextInt(1000);
            int spend = (int) (clickCount * 0.5);
            mainClient.callProcedure(theCallback, "report_bids", campaignId, adId, clickCount, spend, campaignId);

            if (eventCount++ % 100000 == 1) {
                msg("Reported " + eventCount + " spending events...");
            }

            if (System.currentTimeMillis() > nextOverspendReportMs) {
                showOverspends(mainClient);
                nextOverspendReportMs = System.currentTimeMillis() + (1000 * globalQueryFreqSeconds);
            }

        }

        msg("Reported " + eventCount + " spending events...");

        // Because we've put messages into the clients queue we
        // need to wait for them to be processed.
        msg("All " + campaignCount + " entries in queue, waiting for it to drain...");
        mainClient.drain();

        final double entriesPerMs = (campaignCount) / (System.currentTimeMillis() - startMsBenchmark);
        msg("Proceseed " + entriesPerMs + " events per ms...");
        showOverspends(mainClient);

        reportRunLatencyStats(tpMs, entriesPerMs);
    }

    private static void showOverspends(Client mainClient)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = mainClient.callProcedure("@AdHoc",
                "select count(*) how_many from campaign_overspends where spend > budget;");

        msg("Campaigns that have spent too much:");
        msg(cr.getResults()[0].toFormattedString());

    }

    /**
     * Turn latency stats into a grepable string
     *
     * @param tpMs target transactions per millisecond
     * @param tps  observed TPS
     */
    private static void reportRunLatencyStats(int tpMs, double tps) {
        StringBuffer summary = new StringBuffer("GREPABLE SUMMARY:");

        summary.append(tpMs);
        summary.append(':');

        summary.append(tps);
        summary.append(':');
        summary.append(System.lineSeparator());

        summary.append("Percentiles are in Microseconds - divide by 1000 to get milliseconds...");
        summary.append(System.lineSeparator());

        summary.append(SafeHistogramCache.getInstance().get(DELETE_CAMPAIGN).toStringShort());
        summary.append(System.lineSeparator());

        summary.append(SafeHistogramCache.getInstance().get(CREATE_CAMPAIGN).toStringShort());
        summary.append(System.lineSeparator());

        summary.append(SafeHistogramCache.getInstance().get(CREATE_CAMPAIGN_ADS).toStringShort());
        summary.append(System.lineSeparator());

        summary.append(SafeHistogramCache.getInstance().get(RUN_CAMPAIGN).toStringShort());
        summary.append(System.lineSeparator());

        msg(summary.toString());

    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 7) {
            msg("Usage: hostnames campaigncount tpms durationseconds queryinterval budget adcount");
            System.exit(1);
        }

        // Comma delimited list of hosts...
        String hostlist = args[0];
        msg("hostlist=" + hostlist);

        // How many users
        int campaignCount = Integer.parseInt(args[1]);
        msg("campaignCount=" + campaignCount);

        // Target transactions per millisecond.
        int tpMs = Integer.parseInt(args[2]);
        msg("tpMs=" + tpMs);

        // Runtime for TRANSACTIONS in seconds.
        int durationSeconds = Integer.parseInt(args[3]);
        msg("durationSeconds=" + durationSeconds);

        // How often we do global queries...
        int globalQueryFreqSeconds = Integer.parseInt(args[4]);
        msg("globalQueryFreqSeconds=" + globalQueryFreqSeconds);

        // campaign budget
        int budget = Integer.parseInt(args[5]);
        msg("budget=" + budget);

        // campaign budget
        int adCount = Integer.parseInt(args[6]);
        msg("adCount =" + adCount);

        try {
            // A VoltDB Client object maintains multiple connections to all the
            // servers in the cluster.
            Client mainClient = connectVoltDB(hostlist);
            deleteOldCampaigns(tpMs, mainClient);
            createNewCampaigns(campaignCount, tpMs, mainClient, budget, adCount);
            runOverspendBenchmark(campaignCount, tpMs, mainClient, durationSeconds, globalQueryFreqSeconds, adCount);

            msg("Closing connection...");
            mainClient.close();

            System.exit(0);

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

}
