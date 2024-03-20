

on Linux - ubuntu 22.04 
------------------------------------------------------

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

---

install scala
```bash
sudo apt install scala
```

verify the installation
```bash
scala -version
```

--- 


install python
```bash
sudo apt install python3.10
```

verify the installation
```bash
python3 --version
```

---

download spark from the official website
```bash
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
```

---

set the SPARK_HOME environment variable
```bash
echo "export SPARK_HOME=~/spark-3.5.1-bin-hadoop3" >> ~/.bashrc
source ~/.bashrc
```

---

verify the installation by running the spark-shell

for scala development
```bash
$SPARK_HOME/bin/spark-shell
```

---

verify the installation by running the pyspark

set the PYSPARK_PYTHON environment variable
```bash
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc
```

for python development
```bash
$SPARK_HOME/bin/pyspark
```

---


Summary:

- install java 8 or 11
- set the JAVA_HOME environment variable
- install scala
- install python
- download spark from the official website
- set the SPARK_HOME environment variable
- verify the installation by running the spark-shell
- set the PYSPARK_PYTHON environment variable
- verify the installation by running the pyspark

---


------------------------------------------------------

1. pycharm

    - download pycharm community edition
    - install pycharm
    - open pycharm
    - create a new project
    - create a new python file
    - write the code
    - run the code

------------------------------------------------------

------------------------------------------------------

2. visual studio code

    - download visual studio code
    - install visual studio code
    - open visual studio code
    - create a new project
    - create a new python file
    - write the code
    - run the code

how to create python virtual environment in terminal
```bash
python3 -m venv venv
source venv/bin/activate
```
------------------------------------------------------

3. Notebooks

    - jupyter notebook
    - jupyter lab
    - google colab
    - databricks notebook
    - zeppelin notebook

    option-1 : How to install jupyter notebook
    ```bash
    pip install jupyter
    jupyter notebook
    ```

    option-2 : Vscode juptyer notebook extension

------------------------------------------------------

# Spark Environment Setup on Windows

- check the java version
```bash
java -version
```

- check the python version
```bash
python --version
```

- install pyspark
```bash
pip install pyspark
```


- download hadoop for windows
https://github.com/steveloughran/winutils
- set the HARDOOP_HOME environment variable to the winutils.exe path


- download spark from the official website
- set the SPARK_HOME environment variable





