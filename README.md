# telco-anomaly-detection-spark
Anomaly Detection in Telcos with Spark


[Specifications Available in the Wiki](https://github.com/mapr-demos/telco-anomaly-detection-spark/wiki/Specifications)

## Run on cluster

1. ssh to the cluster machine.
2. If java7 or lower version is installed on cluster, install java8.
    Tutorial(use commands with sudo) => http://tecadmin.net/install-java-8-on-centos-rhel-and-fedora/.
3. Clone the git repo with project to `/home/vagrant/telco-anomaly-detection-spark`.
4. Create streams, topics and directories on the cluster machine:
  ```
  sudo -u mapr -s
  # create streams
  maprcli stream delete -path /telco
  maprcli stream create -path /telco 
  maprcli stream edit -path /telco -produceperm u:1000 -consumeperm u:1000 -topicperm u:1000
  maprcli stream topic create -path /telco -topic fail_tower
  maprcli stream topic create -path /telco -topic init
  maprcli stream topic create -path /telco -topic move
  maprcli stream topic create -path /telco -topic event
  for i in `seq 1 20`; do
      maprcli stream topic create -path /telco -topic tower$i
  done
  
  # check if topic creation was successful
  maprcli stream topic list  -path /telco 
  
  # create required directories
  hadoop fs -mkdir /apps/telco
  hadoop fs -chmod 777 /apps/telco
  hadoop fs -mkdir /apps/telco/db
  hadoop fs -chmod 777 /apps/telco/db
  
  exit
  ```
5. Copy configuration and add permissions for the `run.sh`:

  ```
  # go to the folder telco-anomaly-detection-spark
  cd /home/vagrant/telco-anomaly-detection-spark
  chmod 777 run.sh
  cp common/src/main/resources/config.conf /tmp/
  ```
6. Run the start script:
  ```
  sudo -u mapr -s
  cd /home/vagrant/telco-anomaly-detection-spark
  ./run.sh
  ```
7. Now you can open UI in browser:

    - telco ui `http://[cluster-node-ip]:8080/`
    - spark ui `http://[cluster-node-ip]:4040/jobs/#active`
