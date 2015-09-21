# twitter study case
Requirement:
install tweepy
install pymongo
install necessary security packages

set the cursor timeout to be at least one hour when handle big data during query.
(use admin
 db.runCommand({setParameter:1, cursorTimeoutMillis:3600000}))
