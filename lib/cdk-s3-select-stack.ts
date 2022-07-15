import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Bucket} from "aws-cdk-lib/aws-s3";
import {NodejsFunction} from "aws-cdk-lib/aws-lambda-nodejs";
import {LambdaRestApi} from "aws-cdk-lib/aws-apigateway";
import {Runtime} from "aws-cdk-lib/aws-lambda";
import {BucketDeployment, Source} from "aws-cdk-lib/aws-s3-deployment";

export class CdkS3SelectStack extends Stack {
    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        //Creating a S3 bucket
        const s3Bucket = new Bucket(this, "EmployeeS3Bucket");

        //Uploading the sample csv file
        new BucketDeployment(this, "EmployeeSampleCsv",{
            sources: [Source.asset("data")],
            destinationBucket: s3Bucket,
            retainOnDelete: false
        });

        //Creating a Lambda function as the target for API gateway
        const s3SelectFunction = new NodejsFunction(this, "S3SelectFunction", {
            runtime: Runtime.NODEJS_14_X,
            handler: "handler",
            entry: "./lambda/s3-select.js",
            environment: {
                BUCKET_NAME: s3Bucket.bucketName,
                OBJECT_NAME: "employee.csv",
                REGION: Stack.of(this).region
            }
        });

        s3Bucket.grantRead(s3SelectFunction);

        //Integrating API gateway with Lambda function
        const apiGateway = new LambdaRestApi(this, "RestAPI", {
            handler: s3SelectFunction
        });

    }
}
