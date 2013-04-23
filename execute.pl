#!/usr/bin/perl

# Simple script to start a Node Manager that uses a compiled lib.jar

main();

sub main {
    
    $classpath = "proj/:jars/plume.jar:jars/lib.jar:jars/java-json.jar";
    
    $args = join " ", @ARGV;

    exec("java -agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=n -ea -cp $classpath edu.washington.cs.cse490h.lib.MessageLayer $args");
}

