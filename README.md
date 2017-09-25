# enclave-server

About:  
The program is used to host groups for the Enclave app for Android (https://play.google.com/store/apps/details?id=com.jain.udbhav.enclave). It can host a maximum of 8 users at once. Users that have not received messages for a period of 3 continuous days are automatically kicked out. Manual kick-outs can be performed using the 'kick' command (kick user_name). Database cleanup is performed every 15 minutes. Communications between clients and servers are protected by SSL technology.  

Requirements:  
1. Java Runtime Environment 1.8  
2. MySQL server  

How to run:  
1. Create a keystore file:  
https://docs.oracle.com/cd/E19509-01/820-3503/ggfen/  

2. Generate a certificate for your keystore. Portecle can be used for this:  
http://portecle.sourceforge.net/  

3. Send the certificate to app users.  

4. Run Enclave.jar  
Usage: java -jar Enclave.jar database_address keystore_filename port_number  
The program will then ask for your database username, password and the keystore password.  

Example: java -jar Enclave.jar localhost:3306/enclave server.jks  

Note:  
1. It is highly recommended to create a new, empty database for the server.  
2. The dependency jars(JSON and MySQL connector) should be in the same folder as Enclave.jar.  
3. Do not make manual changes to the database.    
4. Ensure that the port you are using has been forwarded.   

Licenses:    
JSON by JSON.org    
http://www.json.org/license.html   



