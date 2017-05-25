### PatChain 

Patchain (a.k.a Partition Chain) is a tamper-resistant data model that ensures data objects are chained together such that each objects references the object before it. This is similar to how transactions in a blockchain are cryptographically linked together. 

Patchain defers from the traditional blockchain design in that it is meant to be used on centralized/distributed environment and designed on traditional ACID-complaint databases. It does not organize objects into blocks but into logical units known as a `Partition`.

A Partition is a logical collection of objects that are linked together. The first object of a partition references the hash of the partition and the next references the hash of the object before it. For improved performance and reduction in lock contention during chaining, systems making use of this model will need to create multiple partitions to improve write through-put. A system composed of multiple partitions will also have all partitions linked to each other.

![Patchain Illustrated](https://storage.googleapis.com/krogan/patchain_img.png)

As seen in the image above, all objects are shared between two partitions (P1 and P2) and each individual object in the partitions link to the object before it. The first object of each partition references the hash of the partition it is assigned to. Furthermore, we see the second partition (P2) link to the partition (P1). This is necessary to maintain data integrity across partitions. 

This repository contains a Patchain implementation on [CockroachDB](https://www.cockroachlabs.com). Please see tests for examples.