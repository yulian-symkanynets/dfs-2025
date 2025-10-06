#!/bin/sh

# Testing Labs

# Check if Java is installed
if java -version 2>&1 >/dev/null | grep -q "java version\|openjdk version" ; then
  :
else
  echo "Java is not installed!"
  exit 1
fi

# Check Lab number
if [ "$1" != "1" ] && [ "$1" != "2" ] && [ "$1" != "3" ] && [ "$1" != "4" ]; then
  echo "Wrong Lab number, use only 1 - 4 numbers!"
  exit 1
fi

# Run grading system
java -jar bin/grading-test-0.9.jar dfs-test-0.10.jar dfs.tests.Lab${1}
