hadoop fs -mkdir colbySnedekerProject1/input
hadoop fs -put sample.txt colbySnedekerProject1/input

mkdir runningMean_class
javac -classpath /opt/hadoop/hadoop-core-1.2.1.jar:/lib/commons-cli-1.2.jar -d runningMean_class RunningMean.java
jar -cvf RunningMean.jar -C /home/vcslstudent/colbySnedekerProject1/runningMean_class/ .
hadoop jar /home/vcslstudent/colbySnedekerProject1/RunningMean.jar snedeker.cc.project1.cluster.RunningMean 3 /user/vcslstudent/colbySnedekerProject1/input/sample.txt /user/vcslstudent/colbySnedekerProject1/RunningMean3_out_java
hadoop jar /home/vcslstudent/colbySnedekerProject1/RunningMean.jar snedeker.cc.project1.cluster.RunningMean 4 /user/vcslstudent/colbySnedekerProject1/input/sample.txt /user/vcslstudent/colbySnedekerProject1/RunningMean4_out_java

echo -e "\n\nWindow Size of 3:\n"
hadoop fs -cat /user/vcslstudent/colbySnedekerProject1/RunningMean3_out_java/part-r-00000
echo -e "\n\nWindow Size of 4:\n"
hadoop fs -cat /user/vcslstudent/colbySnedekerProject1/RunningMean4_out_java/part-r-00000