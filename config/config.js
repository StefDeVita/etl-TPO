import { sql, poolPromise } from './db.js';
import AWS from './aws.js';
export const config = {
    AWS: AWS,
    sql: sql,
    poolPromise: poolPromise,
    AWS_SQS_QUEUE_URL: process.env.AWS_SQS_QUEUE_URL
}