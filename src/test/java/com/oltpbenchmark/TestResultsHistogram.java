package com.oltpbenchmark;

import junit.framework.TestCase;

public class TestResultsHistogram extends TestCase {
    public void testAdd() {
        ResultStats.Histogram hist = new ResultStats.Histogram();

        for (int i = 0; i < 1; ++i) {
            hist.add(0);
        }

        for (int i = 0; i < 2; ++i) {
            hist.add(1);
        }

        for (int i = 0; i < 3; ++i) {
            hist.add(2);
        }

        for (int i = 0; i < 4; ++i) {
            hist.add(4);
        }

        for (int i = 0; i < 5; ++i) {
            hist.add(5);
        }

        for (int i = 0; i < 6; ++i) {
            hist.add(10000);
        }

        for (int i = 0; i < 7; ++i) {
            hist.add(11000);
        }

        assertEquals(28, hist.totalCount());
        assertEquals(1, hist.count(0));
        assertEquals(9, hist.count(1));
        assertEquals(9, hist.count(2));
        assertEquals(9, hist.count(4));
        assertEquals(5, hist.count(5));
        assertEquals(13, hist.count(10000));
    }

    public void testAddHistogram() {
        ResultStats.Histogram hist1 = new ResultStats.Histogram();
        hist1.add(0);
        hist1.add(1);
        hist1.add(3);
        hist1.add(2500);
        hist1.add(10000);

        ResultStats.Histogram hist2 = new ResultStats.Histogram();
        hist2.add(2);
        hist2.add(6);

        hist1.add(hist2);
        assertEquals(1, hist1.count(0));
        assertEquals(3, hist1.count(1));
        assertEquals(3, hist1.count(2));
        assertEquals(1, hist1.count(2500));
        assertEquals(1, hist1.count(100000));
    }
}
