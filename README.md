Term-Query Graph for Search Query Recommendation
================================================

A Scala implementation of a Term-Query Graph for search query recommendations.

See [Bonchi et al. (WWW 2011)](http://dl.acm.org/citation.cfm?id=1963201), [Bonchi et al. (SIGIR 2012)](http://zola.di.unipi.it/rossano/wp-content/papercite-data/pdf/sigir12.pdf). An implementation similar to this was used for Feild and Allan (SIGIR 2013).

License
=======

See LICENSE in root directory.


Compiling
=========

This package uses Scala 2.10 and SBT (I'm currently using version 0.12.3). 
Compiling is simple; navigate to the main directory in a console and type:

    sbt package

This will create a jar file:

    target/scala-2.10/termquerygraph_2.10-1.0.jar



Usage
=====

There are two stages required to use this package for query recommendations.
They are: 

1. creating a term-query graph
2. running queries 

We elaborate on these below.


Creating a term-query graph
---------------------------

This step assumes that you have a file consisting of query pairs and counts. 
In the term-query graph, queries are nodes and the counts are placed on
directional edges going from `query1` to `query2`. Note that outgoing edges are
normalized for each query. The file should have a header and three tab-separated
columns: `query1`, `query2`, and `count`. Here's an example:

    query1  query2  count
    foo  foobar  103
    foo  bar    200
    foobar  foo bar 19
    ...

There is a fuller example in:

    samples/pairs.tsv

Assuming you have such a file, you can generate the term-query graph using
the `ProcessQueryPairs` class. Here's the usage:

    scala -cp target/scala-2.10/termquerygraph_2.10-1.0.jar \
        edu.umass.ciir.tqgraph.ProcessQueryPairs
    
    Usage: ProcessQueryPairs <query pair file> <out dir> [<query count est>]
    
     Produces three files with tab-delimited columns in <out dir>:
    
       term-postings.tsv.gz
       query-id-map.tsv.gz
       query-rewrite-matrix.tsv.gz


E.g., to create a term-query graph for our sample pairs and store it in 
`samples/graph-data`, do:

    scala -cp target/scala-2.10/termquerygraph_2.10-1.0.jar \
        edu.umass.ciir.tqgraph.ProcessQueryPairs \
        samples/pairs.tsv \
        samples/graph-data

If you have a lot of queries in the query pairs file, and you know how many, 
then supplying that number as the third argument to `ProcessQueryPairs` should
make things a little faster (it helps when building the internal hash maps).


