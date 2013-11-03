AWS Utils
=========

A simple utility to upload files to Amazon S3 in multiple parts, with resumption.

usage:
```
    ru.vavinov.aws.Main --upload <bucket> <key> <data-file> <status-file>
    ru.vavinov.aws.Main --resume-upload <status-file>
```
example:
```
$ java -Daws.accessKeyId=... -Daws.secretKey=... -cp lib/\*:target/aws-util-1.0-SNAPSHOT.jar ru.vavinov.aws.Main \
    --upload BUCKETNAME scala-2.10.3.tgz ~/Downloads/scala-2.10.3.tgz scala-2.10.3.tgz.status.json
Data file size=30531249, will use part size=5242880
Got uploadId=RV60do7MBbTAEUQ56xOaAWVyZsZ1wTTwOeDG.4RrkbNc3pIP37rSbzbbvLb.lGeS.qq2mh1RzN.UV0wLGuKiQ16pDAOTB9U3DB5Qz1NA2FcQ6vMl6Tg_JpxPfHfC.aqi
Total parts=6, need to upload=6
Uploading part #1.... ETag=15d3a75ba575028c8a62648abba83d90, at bytes-per-second=250651 ETA in 0:01:40
Uploading part #2.... ETag=90796954ff68d2728a79c8fae55abff6, at bytes-per-second=214161 ETA in 0:01:33
Uploading part #3..
^C
$ java -Daws.accessKeyId=... -Daws.secretKey=... ru.vavinov.aws.Main --resume-upload scala-2.10.3.tgz.status.json
Total parts=6, need to upload=4
Uploading part #3.... ETag=2d135fa7709b22cb1bf5d7aac1396550, at bytes-per-second=214828 ETA in 0:01:08
Uploading part #4.... ETag=22e5ed7658224efb40c93277c35baa36, at bytes-per-second=232026 ETA in 0:00:41
Uploading part #5.... ETag=5bc4e9c708682093c0e1ff8a851e5ae5, at bytes-per-second=264644 ETA in 0:00:16
Uploading part #6.... ETag=b5777d5a56613964cbe766d7d57b93e8, at bytes-per-second=188599 ETA in 0:00:00
All parts are uploaded, completing multipart upload...
Multipart ETag: 88ae753299705760a3213a8e2ba1d33b-6
Location: https://BUCKETNAME.s3.amazonaws.com/scala-2.10.3.tgz
```

Requires JDK 1.8.
