# Distributed File System

## Grading System

Version 0.10

### Prerequisites

DFS Grading System is a Java application. Install Java Runtime Environment version 17 or newer and setup PATH environment variable to allow running Java.

### Setup start and stop scripts of your services

Before you run evaluation and grading of your project, you have to setup scripts for starting and stopping servers of your services you have developed. You can provide any suitable way to start and stop servers of your services that follows the technology you used for development and deployment. Please, follow the instructions written in the sample script files to use correct input parameters for starting the servers on a given port and stopping the servers on a given address.  

Lock Service
- `lockservice.ps1` script on Windows computers.
- `lockservice.sh` script on Linux or Mac computers.
- Start and stop Lock Service server on a given port.
- Used in Lab #1 - Lab #4.

Extent Service
- `extentservice.ps1` script on Windows computers.
- `extentservice.sh` script on Linux or Mac computers.
- Start and stop Extent Service server on a given port and a given extent root path.
- Used in Lab #1 - Lab #4.

DFS Service
- `dfsservice.ps1` script on Windows computers.
- `dfsservice.sh` script on Linux or Mac computers.
- Start and stop DFS Service server on a given port and connecting to Extent Service and Lock Service servers on given addresses.
- Used in Lab #1 - Lab #4.

Replica Lock Service
- `replicalockservice.ps1` script on Windows computers.
- `replicalockservice.sh` script on Linux or Mac computers.
- Start and stop Replica Lock Service server on a given port and connecting to Lock Service and primary Replica Lock Service servers on given addresses.
- Used only in Lab #4.

Proxy Lock Service
- `proxylockservice.ps1` script on Windows computers.
- `proxylockservice.sh` script on Linux or Mac computers .
- Start and stop Proxy Lock Service server on a given port and connecting to Lock Service on a given address.
- Used only in Lab #4.

### Run evaluation and grading

When your start and stop scripts are ready and working, you can start evaluation and grading of your project. Run `test.ps1` script on Windows computers or `test.sh` script on Linux or Mac computers. Input parameter is the number of a Lab you want to test (1 - 4).

For example command:

`.\test.ps1 1` (on Windows)

or

`./test.sh 1` (on Linux or Mac)

will run evaluation and grading of Lab #1 of your project.

When prompted, please, enter your student login for your identification. This will help the grading system to keep track of the students. The grading system consumes stdout and stderr of your services during test run. When you want to log the activity of your services, use file(s) to save their outputs.


Be patient, evaluation takes several minutes. When it is finished, you will see the test result and grading of the Lab. All results and gradings are automatically sent to your teacher, you do not need to report anything separately.
