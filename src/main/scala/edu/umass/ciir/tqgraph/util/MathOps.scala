// File:    MathOps.scala
// Author:  Henry Feild
// Date:    21-Dec-2012
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph.util

/**
 * Provides miscellaneous math operations.
 *
 * @author hfeild
 */
object MathOps {

    /**
     * Sums a set of log values. The output is the equivalent of exp each
     * element, adding them together, and then logging. See:
     * http://lingpipe-blog.com/2009/06/25/log-sum-of-exponentials/
     * 
     * @param logValues The values to sum.
     * @return The log-sum-exp of the given values.
     */
    def logSumExp(logValues:Iterable[Double]):Double = {
        val max = logValues.max
        var sum = 0.0

        logValues.foreach{score => 
            if( score != Double.NegativeInfinity )
                sum += math.exp(score - max)
        }
        max + math.log(sum)
    }
}