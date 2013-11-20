/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.boisestate.cs597.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.boisestate.cs597.model.Crime;

/**
 *
 * @author reuben
 */
public class DateComparator {

    public static class CrimeDateComparator extends WritableComparator {

        protected CrimeDateComparator()
        {
            super(Crime.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            Crime k1 = (Crime) w1;
            Crime k2 = (Crime) w2;
            return k1.getDate().compareTo(k2.getDate());
        }
    }

    public static class CrimeDateGrouper extends WritableComparator {

        protected CrimeDateGrouper()
        {
            super(Crime.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            Crime k1 = (Crime) w1;
            Crime k2 = (Crime) w2;
            return k1.getDate().compareTo(k2.getDate());
        }
    }
}
