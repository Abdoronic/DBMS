# This builds all the DBMS and runs the DBAppTest Class.

all: build-DB run-DBAppTest

build-DB: 
	javac src/Dat_Base/*.java

run-DBAppTest:
	java -classpath bin/ Dat_Base.DBAppTest

clean: 
	rm -r bin/Dat_Base/

