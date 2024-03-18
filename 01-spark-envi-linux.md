


on Linux - ubuntu 22.04 

install java 8 or 11
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

set the JAVA_HOME environment variable
```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```

verify the installation
```bash
java -version
```

install scala
```bash
sudo apt install scala
```

---

install python 3.10
```bash
sudo apt install python3.10
```

verify the installation
```bash
python3 --version
```

---

dowanload spark later version
```bash
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvf spark-3.5.1-bin-hadoop3.tgz
```

---

set the SPARK_HOME environment variable
```bash
echo "export SPARK_HOME=~/spark-3.5.1-bin-hadoop3" >> ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc
source ~/.bashrc
```

---

verify the installation by opening interactive shell

for scala
```bash
spark-shell
```

---

set the PYSPARK_PYTHON environment variable
```bash
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc
```

for python
```bash
pyspark
```


---

Summary:

- install java 8 or 11
- set the JAVA_HOME environment variable
- install scala
- install python 3.10
- set the SPARK_HOME environment variable
- set the PYSPARK_PYTHON environment variable

---