#!/bin/bash

javac -g -cp ./jars/plume.jar:./jars/java-json.jar lib/edu/washington/cs/cse490h/lib/*.java proj/*.java -d bin
cd lib
jar cvf ../jars/lib.jar edu/washington/cs/cse490h/lib/*.class

exit
