package com.salesforce.ouroboros.util;

/*               
 * Copyright (C) 2008-2010 Paolo Boldi, Massimo Santini and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

//RELEASE-STATUS: DIST

public class ConsistentHashFunctionTest {

    @Test
    public void testAdd() {
        ConsistentHashFunction<String> chf = new ConsistentHashFunction<String>();
        final String o0 = "0", o1 = "1", o2 = "2";
        Random r = new Random(1);

        chf.add(o0, 1);
        chf.add(o1, 1);
        chf.add(o2, 2);

        boolean found0 = false, found1 = false, found2 = false;

        for (int i = 0; i < 200; i++) {
            long nextLong = r.nextLong();
            if (chf.hash(nextLong) == o0) {
                found0 = true;
            }
            nextLong = r.nextLong();
            if (chf.hash(nextLong) == o1) {
                found1 = true;
            }
            nextLong = r.nextLong();
            if (chf.hash(nextLong) == o2) {
                found2 = true;
            }
        }

        assertTrue(found0);
        assertTrue(found1);
        assertTrue(found2);
    }

    @Test
    public void testConsistency() {
        final Random r = new Random(1);
        int nBucket = 1 + r.nextInt(4);
        ArrayList<String> bucket = new ArrayList<String>();
        for (int i = 0; i < nBucket; i++) {
            bucket.add(Integer.toString(i));
        }

        ConsistentHashFunction<String> chf = new ConsistentHashFunction<String>();

        for (int i = 0; i < nBucket; i++) {
            chf.add(bucket.get(i), 1);
        }

        for (int t = 0; t < 500; t++) {
            long sample = r.nextLong();
            String a = chf.hash(sample);
            String b = "foo";
            chf.add(b, 1);
            String c = chf.hash(sample);
            assertTrue(c == a || c == b);
            if (c == a) {
                System.out.print("*");
            } else {
                System.out.print("-");
            }
            chf.remove(b);
        }
    }

    @Test
    public void testRemove() {
        ConsistentHashFunction<String> chf = new ConsistentHashFunction<String>();
        final String o0 = "0", o1 = "1", o2 = "2";
        Random r = new Random(1);

        chf.add(o0, 1);
        chf.add(o1, 1);
        assertFalse(chf.add(o1, 1)); // To increase coverage
        chf.remove(o1);
        for (int i = 0; i < 1000000; i++) {
            assertEquals(o0, chf.hash(r.nextLong()));
        }
        chf.add(o1, 1);
        chf.add(o2, 1);
        chf.remove(o1);
        chf.remove(o2);
        for (int i = 0; i < 1000000; i++) {
            assertEquals(o0, chf.hash(r.nextLong()));
        }
        chf.remove(o2); // To increase coverage

    }

    @Test
    public void testSecondChance() {
        final Random r = new Random(1);
        int nBucket = 1 + r.nextInt(4);
        ConsistentHashFunction<String> chf = new ConsistentHashFunction<String>();
        for (int i = 0; i < nBucket; i++) {
            chf.add(Integer.toString(i), 1);
        }

        for (int t = 0; t < 500; t++) {
            long sample = r.nextLong();
            List<String> chances = chf.hash(sample,
                                            Math.min(chf.buckets().size(),
                                                     r.nextInt(3) + 2));
            System.out.println("Chances for " + sample + " are " + chances
                               + " out of " + chf.buckets());
            for (String chance : chances) {
                assertEquals(chf.hash(sample) + " != " + chance,
                             chf.hash(sample), chance);
                chf.remove(chance);
            }
            for (int i = chances.size() - 1; i >= 0; i--) {
                chf.add(chances.get(i), 1);
                //assertEquals( chf.hash( sample ), chances[ i ] );
            }

            assertTrue(sample + ": " + chances + " != "
                               + chf.hash(sample, chances.size()) + " (size="
                               + chf.buckets().size() + ")",
                       Arrays.equals(chances.toArray(),
                                     chf.hash(sample, chances.size()).toArray()));
        }
    }

    @Test
    public void testSpecial() {
        ConsistentHashFunction<String> chf = new ConsistentHashFunction<String>();
        long sample = -3599839008849623859L;
        chf.add("0", 1);
        chf.add("1", 1);
        chf.add("2", 1);

        List<String> r = chf.hash(sample, 3);
        System.out.println("3: " + r);
        for (String element : r) {
            assertEquals(chf.hash(sample) + " != " + element, chf.hash(sample),
                         element);
            chf.remove(element);
        }
        for (int i = r.size() - 1; i >= 0; i--) {
            chf.add(r.get(i), 1);
        }

        System.out.println(chf.hash(sample, 3));
    }

    @Test
    @SuppressWarnings("boxing")
    public void testStress() {
        final Random r = new Random(1);
        int nBucket = 1 + r.nextInt(1000);
        ArrayList<String> bucket = new ArrayList<String>();
        for (int i = 0; i < nBucket; i++) {
            bucket.add(Integer.toString(i));
        }

        ConsistentHashFunction<String> chf = new ConsistentHashFunction<String>();

        for (int i = 0; i < nBucket; i++) {
            chf.add(bucket.get(i), 1);
        }

        for (int t = 0; t < r.nextInt(1000); t++) {
            for (int p = 0; p < r.nextInt(100); p++) {
                assertTrue(bucket.contains(chf.hash(r.nextLong())));
            }

            int removals = Math.min(r.nextInt(5), bucket.size() - 1);
            for (int k = 0; k < removals; k++) {
                System.out.printf("Removing %d/%d\n", k, removals);
                String x = bucket.remove(r.nextInt(bucket.size()));
                chf.remove(x);
            }
            int additions = r.nextInt(5);
            for (int k = 0; k < additions; k++) {
                System.out.printf("Adding %d/%d\n", k, additions);
                String x = Integer.toString(new Object().hashCode());
                bucket.add(x);
                chf.add(x, 1);
            }
            if (bucket.size() == 0) {
                System.out.println("Adding out of emergency");
                String x = Integer.toString(new Object().hashCode());
                bucket.add(x);
                chf.add(x, 1);
            }
            assertEquals(bucket.size(), chf.buckets().size());
        }
    }

}