#!/usr/bin/perl

# Simple script to start a Node manager using class files generated by eclipse

main();

sub main {
    
    $classpath = "./bin/:./jars/plume.jar:./jars/java-json.jar";
    
    $args = join " ", @ARGV;

    exec("java -cp $classpath edu.washington.cs.cse490h.lib.MessageLayer $args");
}

