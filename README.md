# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project purpose
Build a data pipeline:
* Get json files from Amazon S3 to load into Redshift the staging tables
* Implement fact and dimension tables via staging tables

# The role of this database in the context of Sparkify
* Provide a good management to the ower.
* Analyse this database can help the startup adjust their behaviors and products to fit with users

# Schema
![schema](Song_ERD.png)
