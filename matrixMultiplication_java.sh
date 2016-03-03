hadoop fs -mkdir colbySnedekerProject2/input
hadoop fs -put input.txt colbySnedekerProject2/input

mkdir matrixMultiplication_class
javac -classpath /opt/hadoop/hadoop-core-1.2.1.jar:/lib/commons-cli-1.2.jar -d matrixMultiplication_class MatrixMultiplication.java
jar -cvf MatrixMultiplication.jar -C /home/vcslstudent/colbySnedekerProject2/matrixMultiplication_class/ .
hadoop jar /home/vcslstudent/colbySnedekerProject2/MatrixMultiplication.jar snedeker.cc.project2.MatrixMultiplication 10 5 /user/vcslstudent/colbySnedekerProject2/input/input.txt /user/vcslstudent/colbySnedekerProject2/MatrixMultiplication_out_java

hadoop fs -cat /user/vcslstudent/colbySnedekerProject2/MatrixMultiplication_out_java/part-r-00000