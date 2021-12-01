async function listReportFilesInS3(s3ErrorReportBucketName, errorReportPrefix) {
    const params = {
        Bucket: s3ErrorReportBucketName,
        Prefix: errorReportPrefix
    }
    try {
        const data = await s3Client.listObjects(params).promise();
        return data.Contents;
    } catch (err) {
        console.log("List S3 error report files failed: ", err);
    }
}

async function readErrorReport(object, reportBucket) {
    const params = {
        Bucket: reportBucket,
        Key: object.Key
    }
    try {
        const data = await s3Client.getObject(params).promise();
        return data.Body;
    } catch (err) {
        console.log("Read S3 error report failed: ", err);
    }
}

module.exports = {listReportFilesInS3, readErrorReport}