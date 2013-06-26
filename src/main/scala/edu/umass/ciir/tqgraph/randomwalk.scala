// File:    randomwalk.scala
// Author:  Henry Feild
// Date:    19-Apr-2013
// Purpose: Provides a serial and parallel implementation of an in-memory
//          random walk with restart (or personalized page rank).
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Random walk with restart (also: personalized page rank). Solves for p in the
 * formula:
 * 
 *      p = (1-c)*A*p + c*e
 *
 * Nodes and edges for the transition matrix A and nodes for the initialization
 * vector e are added one at a time.
 * 
 * @param c                   The restart factor.
 * @param maxIteration        The maximum number of iterations to perform.
 * @param convergenceDistance The L1 distance between two consecutive iterations
 *                            of p(t) and p(t+1) that constitutes convergence.
 *                            If L1(p(t), p(t+1)) is <= convergenceDistance,
 *                            no additional iterations will be performed.
 * @author hfeild
 */
abstract class RandomWalker(val c:Double, 
        val maxIterations:Int=RandomWalker.MaxIterations,
        val convergenceDistance:Double=RandomWalker.ConvergenceDistance) {
    val p:HashMap[Int,PEntry]
    val e:ListBuffer[EEntry]
    var eSum:Double

    /**
     * Generates a function with which you can add out going edges for a node.
     * 
     * @param srcNode      The id of the src node.
     * @return A function, which takes the following parameters:
     *                     @param destNode    The id of the destination node.
     *                     @param prob        The probability, or weight, of the
     *                                        transition.
     */
    def addNodeTransitionsFnc(srcNode:Int):(Int,Double)=>Unit

    /**
     * Performs one step of the random walk.
     */
    def step()

    /**
     * Performs the random walk with restarts. Stops when p converges or the
     * minimum difference is met (see convergenceDistance).
     *
     * @return An object that reports the number of iterations as well as the
     *         time it took to perform the random walk.
     */
    def run():RunInfo = {
        var keepGoing = true
        var iterations = 0
        var distance = 0.0
        Console.err.print( "Iterating: " )

        normalizeE()

        val start = System.currentTimeMillis
        // Iterate until convergence.
        while( keepGoing ) {
            if( iterations > 0 ) {
                if( iterations % 2 == 0 )
                    Console.err.print( "." )
                if( iterations % 25 == 0 )
                    Console.err.print( iterations +"("+ distance +")" )
            }
            step()
            iterations += 1
            distance = updateP().abs

            keepGoing = iterations < maxIterations && 
                distance > convergenceDistance
        }
        val end = System.currentTimeMillis

        new RunInfo(iterations, end-start)
    }

    /**
     * Adds an initialization value for the given id. Also sets the 
     * corresponding value in p to match.
     *
     * @param id     The id of the node.
     * @param value  The value corresponding to that node.
     */
    def addE(id:Int, value:Double) {
        if( !p.contains(id) )
            p(id) = new PEntry(value)
        p(id).value = value
        e += new EEntry(value*c, p(id))
        eSum += value
    }

    /**
     * Normalizes all values in e so they sum to 1. Also updates the 
     * corresponding p values.
     */
    def normalizeE() {
        e.foreach{ entry =>
            entry.value /= eSum
            entry.pEntry.value /= eSum
        }
    }

    /**
     * Updates the value field of each PEntry element, plus returns the
     * L1 distance from the previous values.
     */
    def updateP():Double = {
        var sum = 0.0
        p.values.foreach{entry => 
            sum += entry.runningSum - entry.value
            entry.value = entry.runningSum
            entry.runningSum = 0.0
        }
        sum
    }

    /**
     * Resets p, setting all values to 0.
     */
    def resetP() {
        p.values.foreach{entry => 
            entry.value = 0.0
            entry.runningSum = 0.0
        }
    }

    /**
     * Resets e, clearing all values, resets p, and resets the sum of the
     * values in e.
     * @type {[type]}
     */
    def resetE() = { e.clear(); resetP(); eSum = 0.0 }

    /**
     * Stores an entry in the A matrix. This corresponds to the transition
     * probability for a particular edge: n1 -> n2.
     * 
     * @param prob     The probability (or weight) of the edge.
     * @param srcEntry The entry in p corresponding to n1.
     * @param sumEntry The entry in p corresponding to n2.
     */
    class AEntry(var prob:Double, val srcEntry:PEntry, val sumEntry:PEntry)

    /**
     * Stores an entry in the p vector for a particular node.
     * 
     * @param value      The value for that node (Default: 0.0).
     * @param runningSum The running sum for this node (Default: 0.0).
     */
    class PEntry(var value:Double=0.0, var runningSum:Double=0.0)

    /**
     * Stores an entry in the initialization vector, e, for a particular 
     * node. Also references the corresponding node in the p vector.
     * 
     * @param value     The value for that node (Default: 1.0)
     * @param pEntry    The corresponding entry in p.
     */
    class EEntry(var value:Double=1.0, var pEntry:PEntry)

    /**
     * Encapsulates the number of iterations and the duration (in milliseconds)
     * that a random walk took to converge to within the desired error bound.
     *
     * @param iterations   The number of iterations.
     * @param duration     The duration in milliseconds.
     */
    class RunInfo(val iterations:Int, val duration:Long) {
        override def toString():String = "Converged after "+ iterations +" iterations in "+
            duration +"ms"
    }
}

object RandomWalker {
    val MaxIterations = 1000
    val ConvergenceDistance = 0.005
}


/**
 * A serialized implementation of an in-memory random walk with restart.
 */
class RandomWalk(
        c:Double, 
        maxIterations:Int=RandomWalker.MaxIterations,
        convergenceDistance:Double=RandomWalker.ConvergenceDistance)
    extends RandomWalker(c, maxIterations, convergenceDistance) {

    val A = ListBuffer[AEntry]()
    val p = HashMap[Int,PEntry]()
    val e = ListBuffer[EEntry]()
    var eSum = 0.0

    /**
     * Generates a function with which you can add out going edges for a node.
     * 
     * @param srcNode      The id of the src node.
     * @return A function, which takes the following parameters:
     *                     @param destNode    The id of the destination node.
     *                     @param prob        The probability, or weight, of the
     *                                        transition.
     */
    def addNodeTransitionsFnc(srcNode:Int):(Int,Double)=>Unit = {
        if( !p.contains(srcNode) )
            p(srcNode) = new PEntry()
        val srcNodeEntry = p(srcNode)
        def addOutlink(destNode:Int, prob:Double){
            if( !p.contains(destNode) )
                p(destNode) = new PEntry()
            val destNodeEntry = p(destNode) // Sum.
            A += new AEntry(prob*(1-c), srcNodeEntry, destNodeEntry)
        }
        addOutlink
    }

    /**
     * Performs one step of the random walk.
     */
    def step(){
        A.foreach{ entry =>
            entry.sumEntry.runningSum += entry.srcEntry.value * entry.prob
        }
        e.foreach{ entry =>
            entry.pEntry.runningSum += entry.value
        }
    }
}



/**
 * A parallelized impelmentation of an in-memory random walk with restarts.
 */
class RandomWalkParallelized(
        c:Double, 
        maxIterations:Int=RandomWalker.MaxIterations,
        convergenceDistance:Double=RandomWalker.ConvergenceDistance,
        splitCount:Int=RandomWalkParallelized.Splits)
    extends RandomWalker(c, maxIterations, convergenceDistance) {

    val splits = ListBuffer[ListBuffer[AEntry]]()
    val eSplits = ListBuffer[ListBuffer[EEntry]]()
    val pSplits = ListBuffer[ListBuffer[PEntry]]()
    var curSplit = 0
    var curESplit = 0
    var curPSplit = 0

    // val A = ListBuffer[ListBuffer[AEntry]]() // Transition matrix
    val p = HashMap[Int,PEntry]() // Final vector
    val e = ListBuffer[EEntry]() // Initialization vector
    var eSum = 0.0

    for(i <- 0 until splitCount){
        splits += ListBuffer[AEntry]()
        eSplits += ListBuffer[EEntry]()
        pSplits += ListBuffer[PEntry]()
    }

    /**
     * Generates a function with which you can add out going edges for a node.
     * 
     * @param srcNode      The id of the src node.
     * @return A function, which takes the following parameters:
     *                     @param destNode    The id of the destination node.
     *                     @param prob        The probability, or weight, of the
     *                                        transition.
     */
    def addNodeTransitionsFnc(srcNode:Int):(Int,Double)=>Unit = {
        if( !p.contains(srcNode) ){
            p(srcNode) = new PEntry()
            pSplits(curPSplit) += p(srcNode)
            curPSplit = (curPSplit + 1) % splitCount
        }
        val srcNodeEntry = p(srcNode)
        //val srcNodeEntries = ListBuffer[AEntry]()
        // += srcNodeEntries
        val srcNodeEntries = splits(curSplit)

        // Advance the split index.
        curSplit = (curSplit + 1) % splitCount

        def addOutlink(destNode:Int, prob:Double){
            if( !p.contains(destNode) ){
                p(destNode) = new PEntry()
                pSplits(curPSplit) += p(destNode)
                curPSplit = (curPSplit + 1) % splitCount
            }
            val destNodeEntry = p(destNode) // Sum.
            srcNodeEntries += new AEntry(prob*(1-c), srcNodeEntry,destNodeEntry)
        }

        addOutlink
    }

    override def addE(id:Int, value:Double) {
        if( !p.contains(id) ){
            p(id) = new PEntry(value)
            pSplits(curPSplit) += p(id)
            curPSplit = (curPSplit + 1) % splitCount
        }
        p(id).value = value
        val curE = new EEntry(value*c, p(id))
        e += curE
        eSum += value

        eSplits(curESplit) += curE
        curESplit = (curESplit + 1) % splitCount
    }

    /**
     * Normalizes all values in e so they sum to 1. Also updates the 
     * corresponding p values.
     */
    override def normalizeE() {
        val futures = eSplits.map{split => future{ blocking {
            split.foreach{ entry =>
                entry.value /= eSum
                entry.pEntry.value /= eSum
            }
        }}}

        Await.result(Future.sequence(futures), 50000 seconds)        
    }


    /**
     * Performs one step of the random walk.
     */
    def step(){
        var futures = splits.map{split => future{ blocking{ processSplit(split) }}}
        Await.result(Future.sequence(futures), 50000 seconds)

        futures = eSplits.map{split => future{ blocking{ updateESplit(split) }}}
        Await.result(Future.sequence(futures), 50000 seconds)
    }

    /**
     * Computes the "(1-c)*A*p" part in (1-c)*A*p + c*e.
     * @type {[type]}
     */
    def processSplit(split:ListBuffer[AEntry]){
        split.foreach{ entry =>
            entry.sumEntry.runningSum += entry.srcEntry.value * entry.prob
        }
    }

    /**
     * Updates the p entries that correspond to each entry in the given segment
     * of the e (initialization) vector. This calculates the "+ c*e" part in
     * (1-c)*A*p + c*e.
     * 
     * @param split  A segment of the e vector.
     */
    def updateESplit(split:ListBuffer[EEntry]){
        split.foreach{ entry =>
            entry.pEntry.runningSum += entry.value
        }
    }

    /**
     * Updates the value field of each PEntry element, plus returns the
     * L1 distance from the previous values.
     */
    override def updateP():Double = {
        var sum = 0.0
        val futures = pSplits.map{split => future{ blocking{ updatePSplit(split) }}}
        Await.result(Future.sequence(futures), 50000 seconds).foreach(sum += _)
        //Console.err.println(" = "+ sum)
        sum
    }

    /**
     * Updates an individual split of p. The running sum for each node in p
     * (computed during the calculation of p*A+e) is moved over to its
     * value member. The difference between the new and old value is also
     * calculated and summed over all entries in this split.
     * @param split  A segment of the p vector.
     * @return The sum of difference between the old and new values of p for
     *         this segment.
     */
    def updatePSplit(split:ListBuffer[PEntry]):Double = {
        var sum = 0.0
        split.foreach{entry =>
            sum += entry.runningSum - entry.value
            entry.value = entry.runningSum
            entry.runningSum = 0.0
        }
        //Console.err.print(sum +" ")
        sum
    }
    /**
     * Resets e, clearing all values, resets p, and resets the sum of the
     * values in e.
     * @type {[type]}
     */
    override def resetE() = { eSplits.foreach{split => split.clear()}; 
        e.clear(); resetP(); eSum = 0.0 }

    
}

/**
 * Some constants for RandomWalk2.
 */
object RandomWalkParallelized {
    val Splits = 4
}


/**
 * A running example.
 */
object RandomWalkExample {

    def main(args:Array[String]) {
        // For the following graph:
        // 1 -> 2:0.3, 3:.3, 4:.3
        // 2 -> 1:.5, 5:.5
        // 3 -> 1:.2, 4:.2, 5:.6
        // 4 -> 5:1
        // 5 -> 1:.25, 3:.25, 4:.5
        
        val rwr = new RandomWalkParallelized(0.2)

        // Add outgoing edges for node 1:
        var addTrans = rwr.addNodeTransitionsFnc(1)
        addTrans(2, .33)
        addTrans(3, .33)
        addTrans(4, .34)

        // Add outgoing edges for node 2:
        addTrans = rwr.addNodeTransitionsFnc(2)
        addTrans(1, .5)
        addTrans(5, .5)

        // Add outgoing edges for node 3:
        addTrans = rwr.addNodeTransitionsFnc(3)
        addTrans(1, .2)
        addTrans(4, .2)
        addTrans(5, .6)

        // Add outgoing edges for node 4:
        addTrans = rwr.addNodeTransitionsFnc(4)
        addTrans(5, 1.0)

        // Add outgoing edges for node 5:
        addTrans = rwr.addNodeTransitionsFnc(5)
        addTrans(1, .25)
        addTrans(3, .25)
        addTrans(4, .5)  

        // Set the initialization vector such that only nodes 1 and 5 have 
        // weight.
        rwr.addE(1, .5)
        rwr.addE(5, .5)


        // Perform the random walk.
        val runInfo = rwr.run()

        // Output the results.
        println(runInfo)
        var sum = 0.0
        rwr.p.foreach{case(id,node) =>
            println(id +" -> "+ node.value)
            sum += node.value
        }
        println("Sum: "+ sum)
    }
}