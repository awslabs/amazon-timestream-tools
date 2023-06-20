import {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    ListObjectsCommand,
    DeleteObjectsCommand, GetObjectCommand
} from "@aws-sdk/client-s3";
import {
    STSClient,
    GetCallerIdentityCommand
} from "@aws-sdk/client-sts";
import zlib from 'zlib';
import { Readable } from 'stream';
import csvParser from "csv-parser";

export class TimestreamDependencyHelper {
    constructor(region) {
        this.s3Client = new S3Client({region: region});
        this.stsClient = new STSClient();
    }

    async getAccount() {
        const params = new GetCallerIdentityCommand({});
        return await this.stsClient.send(params).then(
            (data) => {
                return data.Account;
            },
            (err) => {
                console.log("Error while getting the account.", err);
                throw err;
            }
        )
    }

    async createS3Bucket(bucketName) {
        const params = new CreateBucketCommand({
            Bucket: bucketName
        })
        return await this.s3Client.send(params).then(
            (data) => {
                console.log(`Bucket [%s] created`, bucketName);
            },
            (err) => {
                console.log("Error while creating the bucket.", err);
            }
        )
    }

    async deleteS3Bucket(bucketName) {
        console.log("Deleting S3 Bucket");
        const { Contents } = await this.listS3Objects(bucketName);

        if (Contents && Contents.length > 0) {
            await this.deleteS3Objects(bucketName, Contents);
        }
        const params = new DeleteBucketCommand({
            Bucket: bucketName
        })
        return await this.s3Client.send(params).then(
            (data) => {
                console.log("Bucket [%s] deleted Successfully", bucketName);
            },
            (err) => {
                console.log("Deletion of S3 error report bucket failed: ", err);
        })
    }

    async deleteS3Objects(bucketName, contents){
        const params = new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: contents.map(({Key}) => ({Key}))
            }
        })
        return await this.s3Client.send(params).then((data) => {
            console.log("Objects deleted!");
        }, (err) => {
            console.error("Error while deleting the objects", err);
        });
    }

    async getS3Object(uri) {
        const {bucketName, key} = this.getBucketAndKey(uri);
        const params = new GetObjectCommand({
            Bucket: bucketName,
            Key: key
        })
        const response = await this.s3Client.send(params);
        return await response.Body.transformToString();
    }

    async getS3CSVObject(uri, headers) {
        const {bucketName, key} = this.getBucketAndKey(uri);
        const params = new GetObjectCommand({
            Bucket: bucketName,
            Key: key
        });
        const response = await this.s3Client.send(params);
        try {
            let results = [];
            return await new Promise((resolve, reject) => {
                const stream = Readable.from(response.Body);
                stream.pipe(zlib.createGunzip()).pipe(
                    csvParser({separator: ',', headers: false})
                ).on("data", (chunk) => { results.push(chunk);})
                .on("end", () => { resolve(results);});
        });
        } catch (e) {
            console.error("Error while reading the file", e);
        }
    }

    async listS3Objects(bucketName) {
        const params = new ListObjectsCommand({
            Bucket: bucketName
        })
        return await this.s3Client.send(params).then(
            (data) => {
                console.log("Objects listed!")
                return data;
            },
            (err) => {
                console.error("Unable to list Objects");
            }
        )
    }

    getBucketAndKey(uri) {
        const [bucketName] = uri.replace("s3://", "").split("/", 1);
        const key = uri.replace("s3://", "").split('/').slice(1).join('/');
        return {bucketName, key};
    }

     generateRandomStringWithSize(size) {
        const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
        let result = "";
        for (let i = 0; i < size; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return result;
    }

}