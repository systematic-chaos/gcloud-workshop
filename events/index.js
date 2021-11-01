// gcloud functions deploy events --project gcloudworkshop2021 --region europe-west1 --runtime nodejs14 --trigger-http

import { BigQuery } from '@google-cloud/bigquery';
import { Firestore } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';

const PROJECT = 'gcloudworkshop2021';
const DATASET = 'instagram-batch-streaming';
const COLLECTION = 'events';

const eventRowBuilder = function(rowData) {
    return {
        eventId: rowData.eventId,
        eventTime: rowData.eventTime,
        eventProcessingTime: rowData.createdAt,
        resourceId: rowData.resourceId,
        userId: rowData.userId,
        countryCode: rowData.countryCode
    };
};

const insertDataRow = (projectId, datasetId, tableId, rowBuilder) => {
    const bigquery = new BigQuery({ projectId });
    const table = bigquery.dataset(datasetId).table(tableId);

    return async (data) => {
        const row = rowBuilder(data);
        await table.insert([row]);
        return row;
    };
};

const storeFile = (projectId, nameField) => {
    const storage = new Storage({ projectId });

    async function createBucket() {
        let bucket = storage.bucket(computeTodayDate());
        if (!await bucket.exists()) {
            bucket = (await bucket.create())[0];
        }
        return bucket;
    }

    function computeTodayDate() {
        let today = new Date();
        today = new Date(today.getTime() - today.getTimezoneOffset() * 60000);
        return today.toISOString().substring(0, 10);
    }

    return async (file) => {
        const bucket = await createBucket();
        const contents = JSON.stringify(file);
        const name = file[nameField];
        await bucket.file(name).save(contents);
        return name;
    };
};

const storeDocument = (projectId, collectionName) => {
    const firestore = new Firestore({
        projectId,
        timestampsInSnapshots: true
    });
    const collection = firestore.collection(collectionName);

    return async (document) => {
        return collection.add(document);
    };
};

const bigQuery = insertDataRow(PROJECT, DATASET, COLLECTION, eventRowBuilder);
const cloudStorage = storeFile(PROJECT, COLLECTION.slice(0, -1) + 'Id');
const fireStore = storeDocument(PROJECT, COLLECTION);

const events = async (request, response) => {
    const payload = request.body;
    payload.createdAt = new Date().toISOString();

    let result = { ok: true };
    const asyncTasks = [
        bigQuery(payload).then((row) => {
                const eventId = row.eventId;
                result['eventId'] = eventId;
                return eventId;
            }, (error) => {
                console.error(`BigQuery ${COLLECTION}:`, error);
                throw error;
            }),
        cloudStorage(payload).then((fileName) => {
                result['resp'] = fileName;
                return fileName;
            }, (error) => {
                console.error(`Cloud Storage ${COLLECTION}:`, error);
                throw error;
            }),
        fireStore(payload).then((documentReference) => {
                const docRefId = documentReference.id;
                result['externalId'] = docRefId;
                return docRefId;
            }, (error) => {
                console.error(`Firestore ${COLLECTION}:`, error);
                throw error;
            })
    ];

    const asyncTasksResult = await Promise.allSettled(asyncTasks);
    if (asyncTasksResult.some(resolution => resolution.status === 'fulfilled')) {
        response.status(200).send(result);
    } else {
        response.status(500).send({ error: 'Unable to store data.' });
    }
};

export { events };
