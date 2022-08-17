EPTS MozART II
==========================

Description
-----------
This module facilitates the generation of MozART II from OpenMRS user interface. Making it easy for any user with access
to the administration page of OpenMRS able to generate the database.

Building from Source
--------------------
You will need to have Java 1.6+ and Maven 2.x+ installed.  Use the command 'mvn package' to 
compile and package the module.  The .omod file will be in the omod/target folder.

Alternatively you can add the snippet provided in the [Creating Modules](https://wiki.openmrs.org/x/cAEr) page to your 
omod/pom.xml and use the mvn command:

    mvn package -P deploy-web -D deploy.path="../../openmrs-1.8.x/webapp/src/main/webapp"

It will allow you to deploy any changes to your web 
resources such as jsp or js files without re-installing the module. The deploy path says 
where OpenMRS is deployed.

Installation
------------
1. Build the module to produce the .omod file.
2. Use the OpenMRS Administration > Manage Modules screen to upload and install the .omod file.

If uploads are not allowed from the web (changable via a runtime property), you can drop the omod
into the ~/.OpenMRS/modules folder.  (Where ~/.OpenMRS is assumed to be the Application 
Data Directory that the running openmrs is currently using.)  After putting the file in there 
simply restart OpenMRS/tomcat and the module will be loaded and started.

Running
-------
Once the module is deployed one needs to provide a properties file with a name _mozart2.properties_ . Place this file in
the OpenMRS data directory. The content of file are as follows.
```properties
# jdbc url is mysql connection string via JDBC
# example -> jdbc.url=jdbc:mysql://localhost:3306/openmrs?autoReconnect=true
jdbc.url=jdbc:mysql://<host>:<port>/<database>?autoReconnect=true
 
# db.username is the mysql user who has read access to database
# replace <username> with an actual user account in the database.
db.username=<username>
 
# db.password is the password the defined user above connects to mysql with.
# Replace <password> with an actual password
db.password=<password>
 
# The new mozart2 database
# Replace <new database to be created> with an actual value. Leave blank to use the default which is mozart2
newDb.name=<new database to be created>
 
# Comma separated list of Locations IDs of the locations for which you wish to extract patient data.
# Replace <locations IDs> with the actual values of the locations IDs you wish to extract.
# For example locations.ids=12,23,45
locations.ids=<locations IDS>
    
# The end date to extract records. If this value is not specified the date of the day when the application
# is run is the default end date. Replace <end date> with an actual date or leave it blank to let default 
# take effect.
end.date=<end date>
 
# The pattern of the provided date in the end.date property, if not provided the assumed default is dd-MM-yyyy
# Replace <end date pattern> with the actual pattern or leave blank to use the default.
end.date.pattern=<end date patter>
 

# Whether to drop the newly created database after backup to sql dump file.
drop.newDb.after=true
    
# The batch sizes to optimize the procedure when the records to be generated are too many. (default is 20000)
batch.size=20000
```