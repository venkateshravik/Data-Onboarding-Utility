# Data-Onboarding-Utility
Self Serve Data Onboarding Utility

user :
- login - authentication
- bq connection via external client

    

api : 
- bq connection
- bq upload

to-do :
-  instead of local file buffer use gcs-bucket
- design the
    - homepage
    - login screen
    - password and forgot password screen
    - find a way to authenticate with gcp [service-account ,token or any otherway]
    -  integrate the dataplex apis into data-onboarding-utility

- Get the data from the source table and update it through the user input [ default value and Description ]
- how we are going to handle the new data
    - after user upload shall we creating table for every new users or
    - how we are going to handle this 
- user to jobscan mapping[where to store the JOB_ID]
- user auth [username,password]
- login through google
- 
- deploy this to appengine
