# Ellis: Spark Application Scale-out Tuner

Ellis uses Spark listener to implement its tuning strategy on job level.
It is the implementation of
Thamsen, Lauritz, et al. "Ellis: Dynamically Scaling Distributed Dataflows to Meet Runtime Targets." 2017 IEEE International Conference on Cloud Computing Technology and Science (CloudCom). IEEE, 2017.


* Environment
  - Spark: `v3.0.0`
  - Scala: `2.12.10`
    
* Usage
  
    The entry class is `de.tuberlin.cit.adjustments.EllisScaleOutListener`
    
    You can either set it as an additional listener in Spark (which can be transparent to users), or you can explicitly create it in your Spark applications.