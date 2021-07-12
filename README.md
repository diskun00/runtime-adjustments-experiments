# Ellis: Spark Application Scale-out Tuner

Ellis uses Spark listener to implement its tuning strategy on job level.
It is the implementation of
Thamsen, Lauritz, et al. "Ellis: Dynamically Scaling Distributed Dataflows to Meet Runtime Targets." 2017 IEEE International Conference on Cloud Computing Technology and Science (CloudCom). IEEE, 2017.


* Environment
  - Spark: `v3.1.1`
  - Scala: `2.12.10`
    
* Usage
  
    The entry class is `de.tuberlin.dos.adjustments.EllisScaleOutListener`
    
    You can either set it as an additional listener in Spark (which can be transparent to users), or you can explicitly create it in your Spark applications.

* Dataset Generator
  1. Vandermonde and Points 
    
      The generators are in `/src` folder under package `de.tuberlin.dos.datagens`.  
  
  2. Multiclass
  
      The generator is in `/python` folder.

* Workload
    
  Workloads are located in `de.tuberlin.dos.jobs.v2_1`.