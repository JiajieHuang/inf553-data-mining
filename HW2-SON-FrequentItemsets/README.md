How to run?	
	
1.	Move	the	input	files	and	source	code	file	inside	the	spark-1.6.1-bin-hadoop2.4	
folder	in	your	machine	
2.	In	terminal	enter	the	same	spark-1.6.1-bin-hadoop2.4	directory	and	run	the	
following	command.	
	
Command	to	enter	directory	-->	
	
	
cd	spark-2.2.1-bin-hadoop2.7
	
Command	used	to	run	source	code	-->	
	
./bin/spark-submit	hw2.py	1	ratings.dat	users.dat	1200	
output_task1_1200.txt	
	
./bin/spark-submit	hw2.py	1	ratings.dat	users.dat	1300	
output_task1_1300.txt	
	
./bin/spark-submit	hw2.py	2	ratings.dat	users.dat	500
output_task2_500.txt	
	
./bin/spark-submit	hw2.py	2	ratings.dat	users.dat	600	
Voutput_task2_600.txt
