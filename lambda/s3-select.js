import {S3} from "@aws-sdk/client-s3";

const s3client = new S3({region: process.env.REGION});
const BUCKET_NAME = process.env.BUCKET_NAME;
const OBJECT_NAME = process.env.OBJECT_NAME;

exports.handler = async (event) => {
    let empId, firstName, lastName, department, avg, max = undefined

    if (event.body) ({empId, firstName, lastName, department, avg, max} = JSON.parse(event.body));

    let query = ""
    if (avg || max) {
        query = buildAggregationQuery(max, avg, department);
    } else {
        query = buildNormalQuery(empId, firstName, lastName, department, avg, max)
    }

    const params = {
        Bucket: BUCKET_NAME,
        Key: OBJECT_NAME,
        ExpressionType: "SQL",
        Expression: query,
        InputSerialization: {
            CSV: {
                FileHeaderInfo: "USE",
                RecordDelimiter: "\r\n",
                FieldDelimiter: ",",
            },
        },
        OutputSerialization: {
            CSV: {
                RecordDelimiter: "\n",
                FieldDelimiter: ",",
            },
        },
    };

    try {
        const response = await s3client.selectObjectContent(params);

        if (response.Payload === undefined) {
            return {
                statusCode: 500,
                headers: {"content-type": "application/json"},
                body: JSON.stringify("Error: no payload in response"),
            };
        }

        let records = "";
        for await (const event of response.Payload) {
            if (event.Records && event.Records.Payload) {
                records += Buffer.from(event.Records.Payload).toString("utf-8");
            }
        }
        console.log(records);
        return {
            statusCode: 200,
            headers: {"content-type": "application/json"},
            body: JSON.stringify(records.split("\n")),
        };

    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            headers: {"content-type": "application/json"},
            body: JSON.stringify(error),
        };
    }
}

const buildNormalQuery = (
    empId, firstName, lastName, department, avg, max
) => {
    const query = `SELECT * FROM S3Object s `;
    let conditions = [];

    if (firstName) {
        conditions.push(`WHERE s.firstName like '%${sanitize(firstName)}%'`);
    }

    if (lastName) {
        conditions.push(`WHERE s.lastName like '%${sanitize(lastName)}%'`);
    }

    if (department) {
        conditions.push(`WHERE s.department like '%${sanitize(department)}%'`);
    }

    if (max) {
        conditions.push(`WHERE s.department like '%${sanitize(department)}%'`);
    }

    return query + conditions.join(" AND ");
};

const buildAggregationQuery = (max, avg, department) => {
    if (department) {
        if (max) {
            return `SELECT MAX(CAST(s.salary as INT)) FROM S3Object s WHERE s.department= '${sanitize(department)}'`;
        }

        if (avg) {
            return `SELECT AVG(CAST(s.salary as INT)) FROM S3Object s WHERE s.department= '${sanitize(department)}'`;
        }
    } else {
        if (max) {
            return `SELECT MAX(CAST(s.salary as INT)) FROM S3Object s`;
        }

        if (avg) {
            return `SELECT AVG(CAST(s.salary as INT)) FROM S3Object s`;
        }
    }
}

const sanitize = (str) => str.replace(/[^a-z0-9 ]/gi, "");