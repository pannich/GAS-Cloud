# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

### Directory

```
/aws
  user_data_web_server.txt
  user_data_annotator.txt
/web
  views.py
  config.py
/ann
  /ann_package
  /anntools
    run.py
  ann_config.ini
  annotator.py
  README.md
  run_ann.sh
  run.py       // copy of original
/util
  /ann_package
  /archive
    archive_config.ini
    archive.py
  /restore
    restore_config.ini
    restore.py
  /thaw
    helpers.py
    util_config.ini
  helpers.py
  util_config.ini
  README.py
```

### Submission Note

a. Description of your archive process.
- nichada_job_archives sqs listen to job_results SNS topic with 5 minutes delay. After 5 minutes, non-premium users' files will be downloaded to local and subsequently uploaded to Glacier archive. Save Glacier archive id on dynamoDB then remove that file from s3.
  The file is no longer accessible for free user.
-  Use SQS with 5 mins delay to allow users 5 minutes to download file before proceeding to archive process with Glacier.

b. Description of your restore process.
- I use SNS and SQS to allow web server to communicate with util server. Web server is signaling the user's change of role, and util is processing the restoration process.
- When a user switched to premium, a message is sent to Subscription Topic along with user_id.
- In restore.py, subscription queue is listening to new upgraded user.
  Parse user_id from the new message and filter all jobs from this user from dynamoDB.
- For each job, send Glacier archive id to Thaw SNS topic.
  Also, update db 'restore message' to 'the data is retriving taking a few moment'.
  Since this process will take time. I used the SNS topic to allow constant polling until status changes.
- In Thaw.py, poll to check if any retrieval initations were submitted and Succeeded.
  Download retreival file from Glacier to local and submit that file to s3.
  Update db 'restore message' to ''.
- The web will now show download link to user.


c. Anything else you’d like to share with the grader. Include notes here if you didn’t
manage to complete all the exercises and wish to describe what issues you
encountered that prevented you from doing so.
- After upgrade, the download button wrongly shows up before the retrival msg shows up. It'll show up on refresh.

d. If you completed the optional exercises, include descriptions of what you
observed and why. If you prefer, you may include the auto scaling description and screenshots in a PDF file. Do not include Word documents.
