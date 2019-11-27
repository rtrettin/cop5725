COP5725 Implementation Project
Fall 2019

Directory Structure
- asterixdb
	- contains configuration and startup/shutdown scripts for the AsterixDB cluster used in this implementation
	- used with AsterixDB version 0.9.4.1
- data
	- contains a python script to generate synthetic dirty data
	- contains the border crossing data set from https://www.kaggle.com/akhilv11/border-crossing-entry-data
	- contains various SQL query templates for working with the border entry data
- java
	- contains main source code for the Cleanix system implementation based on "Cleanix: A Parallel Big Data Cleaning System" by Hongzhi Wang et al. from ACM SIGMOD Record 2015 ( https://dl.acm.org/citation.cfm?id=2935694.2935702 ).
		- pom.xml: Maven configuration file used to build the JAR executable from the source files
		- src: full Java source code
		- target: compiled JAR file
- socket
	- Websocket communication server to exchange information between the web interface and the Java Cleanix system
- web
	- user interface for database connection details and rule definitions

Demo URL: https://crustycrab.proxcp.com

The project demo was run on a 3 server cluster:

- Server 1: web interface, websocket server, Cleanix, MySQL sources, AsterixDB server 1
- Server 2: AsterixDB server 2
- Server 3: AsterixDB server 3

Each server was an Ubuntu VM with 2 CPU cores, 2GB RAM, and 50GB HDD storage.

The demo showed how the Cleanix system works with synthetic dirty data and real world dirty data. Both data sets had 20% and 28% of the data dirtied, respectively.

Metrics used to measure effectiveness of the Cleanix system:
- Number of empty values after cleaning vs before cleaning
- Number of abnormal value detection rules hit before cleaning vs after cleaning 1 pass

Data cleanliness is mostly subjective to the user and depends on the data set and types of analytics being performed.

How To Run
This implementation requires at least 1 Linux server with various components:
- AsterixDB
	- Installation documentation: https://asterixdb.apache.org/docs/0.9.4.1/ncservice.html#Small_cluster
	- How to configure: place asterixdb/cc.conf in opt/local/conf/
	- How to run: place asterixdb/startcc.sh onto the server and execute it
- MySQL
	- Required to store the source data set(s)
- Python 3
	- Only required if using the provided synthetic data generator script
	- How to install prerequisites: python -m pip install -r requirements.txt
- NodeJS v10.x
	- How to install prerequisites: a) cd to the directory with the package.json file then b) run "npm install"
	- How to run: node server.js
- Java
	- The jar file can be built using the included src files and pom.xml with the Maven build "clean install" command
	- Run the jar file in java/target/ with: java -jar cop5725-1.0.0-jar-with-dependencies.jar
- Web server
	- Place all files in the web/ directory in your web server directory to use the web interface

Once all components are installed and running correctly, the Java application and the Web interface should be able to connect to the NodeJS websocket server.

The Cleanix system is used with the provided web interface. First use the "DB Connection" tab to input the details for the AsterixDB cluster and MySQL source databases. Ensure the "Save" buttons are clicked after each form is completed. Then go to the "Clean Settings" tab to define the data cleaning rules for each input data source. Again ensure the save buttons are clicked after each form is completed.

In the java application, you will then see the progress of the data cleaning algorithms. The final output can be checked in AsterixDB at http://{server IP}:19001

TO RESET THE WEB INTERFACE, open the browser console (F12 in Chrome) and delete all Application > Local Storage variables.

AsterixDB SQL++ documentation: https://asterixdb.apache.org/docs/0.9.4.1/sqlpp/primer-sqlpp.html AND https://asterixdb.apache.org/docs/0.9.4.1/sqlpp/manual.html 
