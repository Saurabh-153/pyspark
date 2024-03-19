


spark-shell ( pyspark ) with local cluster-manager
------------------------------------------------------

```bash
pyspark --help
pyspark --master local[3] 
```

e.g App

```python
df = spark.read.csv("/home/nag/pyspark/data/sample.csv", header=True, inferSchema=True)
df.show()
```