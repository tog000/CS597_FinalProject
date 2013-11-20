/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package comparators;

import edu.boisestate.cs597.model.Crime;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author reuben
 */
public class IucrComparator {

    public static class CrimeIucrComparator extends WritableComparator {

        protected CrimeIucrComparator()
        {
            super(Crime.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            Crime k1 = (Crime) w1;
            Crime k2 = (Crime) w2;
            return k1.getIUCR().compareTo(k2.getIUCR());
        }
    }

    public static class CrimeIucrGrouper extends WritableComparator {

        protected CrimeIucrGrouper()
        {
            super(Crime.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            Crime k1 = (Crime) w1;
            Crime k2 = (Crime) w2;

            return k1.getIUCR().compareTo(k2.getIUCR());
        }
    }
}
