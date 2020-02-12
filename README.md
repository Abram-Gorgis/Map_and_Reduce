Abram Gorgis homework 2

First download hortonworks and vmware

Open vmware and open hortonworks using vmware

download dblp and extract it

run vm and go to url given on screen in a web browser

click launch and use (username and password) to log in maria_dev

click cog at top of screen and wait for everything to start up

send dblp file over to vm using command

scp -P 2222 <path to file you want to send> maria_dev@sandbox-hdp.hortonworks.com:~/

*THIS COMMAND WILL NOT WORK IF YOU DID NOT SET UP YOUR CUSTOM HOSTS IN WINDOWS USE GIVEN SSH ADDRESS maria_dev@address PORT 2222*

using same command send parser java file given called main.java

now ssh into maria_dev how ever you like here is the address maria_dev@sandbox-hdp.hortonworks.com and port 2222
this will only work if you set up custom hosts in your windows files otherwise you will have to use 
maria_dev@address-given with port 2222 still. Your password will be maria_dev

from here use command javac main.java
then run java main <inputfilename> where input file name is the unparsed dblp.xml file

now this will produce a file called parsed.xml you can use ambari and the url you used earlier to check the vm status and go to file view

here navigate to /user/maria_dev and create a new directory called input

now in terminal type hdfs dfs -put parsed.xml <path to input file you created, if you followed example it will be /user/maria_dev/input>
this will create another copy of the data so at this point feel free to delete the original and new parsed xml at the root

now with the scala program copy the given files to the right directories of intellij or how ever you feel is best to install them and from there you can modify the config file to fit your needs
**put config in resources in /src/main/resources, put classes and objects into /src/main/scala, put tests in /src/test/scala , put build into root, put plugins into /project/target**
import sbt build to get all dependencies 

junit tests dont seem to run automatically with scala so right click /test/scala and click run tests and then in the regular terminal type sbt clean assembly

now this should create an executable .jar file

using the previous command to send files to vm send this file over

now you are ready to run the map and reduce you will need to type in the command

hadoop jar <Name>.jar <input folder path(do not give the actual file give the fold> /user/maria_dev/output (this will create an output folder where the final results will be stored)
or if you are using the config file do hadoop jar <name>.jar

now this might take a few minutes but once it is done you can get the outputfile by going to ambari once again navigating to the newly created output file and downloading the .csv

This will have the results of the map and reduce with histograms to view at top and the top 100 and bottom 100 authors at the bottom of the long list of authors

