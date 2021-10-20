# PySpark and Delta Lake
This repo contains a reference implementation for deploying Spark 3.1.1
and Delta Lake. In addition, it includes a reference implementation
showcasing the feature this architecture offers for acquiring data at scale.

## References
- [Learning Spark](https://www.amazon.com/Learning-Spark-Jules-Damji/dp/1492050040/ref=asc_df_1492050040/?tag=hyprod-20&linkCode=df0&hvadid=459538011055&hvpos=&hvnetw=g&hvrand=9637554060338455232&hvpone=&hvptwo=&hvqmt=&hvdev=c&hvdvcmdl=&hvlocint=&hvlocphy=9026797&hvtargid=pla-918087322526&psc=1)
- [Delta Lake](https://docs.delta.io/latest/index.html)
- [spark-submit](http://spark.apache.org/docs/latest/submitting-applications.html)
- [Spark on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html)

## Index
- [Appendix](#appendix)

## How it Works
![Spark on K8s](https://spark.apache.org/docs/latest/img/k8s-cluster-mode.png)

```shell
./bin/docker-image-tool.sh -t 3.1.1 build
```

## Appendix

1. To view the Spark UI, port-forward to the active driver pod:
```
kubectl port-forward <SPARK_DRIVER_POD_NAME> 4040:4040
```

