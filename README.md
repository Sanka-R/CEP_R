CEP_R
=====

R Integration for WSO2 Complex Event Processor (CEP) using jri

<h2>Instruction for setting up R and rJava on Ubuntu</h2>
1. Add the following line to /etc/apt/source.list where ${version} should be trusty or precise or utopic or lucid according to your Ubuntu release
<br>`deb http://cran.rstudio.com/bin/linux/ubuntu ${version}/`
2. Authenticate
<br>`sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9`
3. Update
<br>`sudo apt-get update`
4. Install R
<br>`sudo apt-get install r-base-dev r-cran-rjava`

<h2>Instruction for setting up the project</h2>


1. Set R_HOME
<br>(eg: `export R_HOME = /usr/lib/R`)
2. Set JRI_HOME 
<br>(eg: `export JRI_HOME = /usr/lib/R/site-library/rJava`)
2. Copy JRI.jar, JRIEngine.jar, REngine.jar files to 
<br> `{CEP_HOME}/repository/components/lib`
3. Include the following line in the `wso2server.sh` file in `{CEP_HOME}/bin` in the last do-while loop 
<br>`-Djava.library.path=$JRI_HOME`
4. Use maven clean install
</ol>

