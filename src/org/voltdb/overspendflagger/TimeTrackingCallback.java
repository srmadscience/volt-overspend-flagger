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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class TimeTrackingCallback implements ProcedureCallback {

    SafeHistogramCache shc;
    final long startNanos = System.nanoTime();
    String statBeingTracked = null;

    public TimeTrackingCallback(SafeHistogramCache shc, String statBeingTracked) {
        this.shc = shc;
        this.statBeingTracked = statBeingTracked;
    }

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {

        if (arg0.getStatus() == ClientResponse.SUCCESS) {

            final long endNanos = System.nanoTime();

            final int thisLatency = (int) ((endNanos - startNanos) / 1000);

            SimpleDateFormat sdfDate = new SimpleDateFormat(" HH:mm:ss");
            Date now = new Date();
            String strDate = sdfDate.format(now);

            shc.report(statBeingTracked, thisLatency, strDate, SimulateOverspends.HISTOGRAM_SIZE_MS);

        } else {
            SimulateOverspends.msg("Error Code " + arg0.getStatusString());

        }
    }

}
