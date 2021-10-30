// gcloud functions deploy users --project gcloudworkshop2021 --region europe-west1 --runtime nodejs14 --trigger-http

import { Firestore } from '@google-cloud/firestore';

const PROJECT_ID = 'gcloudworkshop2021';
const COLLECTION_NAME = 'users';

const fsStoreData = (projectId, collectionName) => {

    const firestore = new Firestore({
        projectId: projectId,
        timestampsInSnapshots: true
    });
    const collection = firestore.collection(collectionName);

    return async (req, res) => {
        const payload = req.body;
        payload.createdAt = new Date().toISOString();
        try {
            const documentReference = await collection.add(payload);
            res.status(200).send({ externalId: documentReference.id });
        } catch (error) {
            console.error(error);
            res.status(500).send({ error: `Unable to store ${collectionName} data.` });
        }
    };
};

const users = fsStoreData(PROJECT_ID, COLLECTION_NAME);

export { users };
