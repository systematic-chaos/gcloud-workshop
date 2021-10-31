// gcloud functions deploy resources --project gcloudworkshop2021 --region europe-west1 --runtime nodejs14 --trigger-http

import { BigQuery } from '@google-cloud/bigquery';
import { Firestore } from '@google-cloud/firestore';

const PROJECT = 'gcloudworkshop2021';
const DATASET = 'instagrambatchstreaming';
const COLLECTION = 'resources';

const resourceRowBuilder = function(rowData) {
    return {
        resourceId: rowData.resourceId,
        resourceName: rowData.resourceName,
        categoryId: rowData.categoryId,
        providerId: rowData.providerId,
        promotion: rowData.promotion,
        createdAt: rowData.createdAt
    };
};

const bqInsertDataRow = (projectId, datasetId, tableId, rowBuilder) => {
    const bigquery = new BigQuery({ projectId });
    const table = bigquery.dataset(datasetId).table(tableId);

    return async (data) => {
        const row = rowBuilder(data);
        await table.insert([row]);
        return row;
    };
};

const fsStoreDocument = (projectId, collectionName) => {
    const firestore = new Firestore({
        projectId,
        timestampsInSnapshots: true
    });
    const collection = firestore.collection(collectionName);

    return async (document) => {
        return collection.add(document);
    };
};

const bigQuery = bqInsertDataRow(PROJECT, DATASET, COLLECTION, resourceRowBuilder);
const fireStore = fsStoreDocument(PROJECT, COLLECTION);

const resources = async (request, response) => {
    const payload = request.body;
    payload.createdAt = new Date().toISOString();

    let result = {};
    const asyncTasks = [
        bigQuery(payload).then((row) => {
                const resourceId = row.resourceId;
                result['resourceId'] = resourceId;
                return resourceId;
            }, (error) => {
                console.error(`BigQuery ${COLLECTION}:`, error);
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
        response.status(500).send({ error: `Unable to store ${COLLECTION} data.` });
    }
}

export { resources };
