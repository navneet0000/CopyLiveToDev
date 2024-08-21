const { MongoClient } = require('mongodb');
require('dotenv').config();

async function copyAndUpdateIndexes(liveUri, devUri) {
    const liveClient = new MongoClient(liveUri);
    const devClient = new MongoClient(devUri);
    try {
        await liveClient.connect();
        await devClient.connect();

        const liveDb = liveClient.db('SalesHive_Live');
        const devDb = devClient.db('SalesHive_Dev');

        const collections = await liveDb.listCollections().toArray();

        for (const collection of collections) {
            const collectionName = collection.name;

            if (collectionName.startsWith('temp') || collectionName.startsWith('system')) {
                console.log(`Skipping collection: ${collectionName}`);
                continue;
            }

            const liveCollection = liveDb.collection(collectionName);
            const devCollection = devDb.collection(collectionName);

            // Retrieve indexes from the live database
            const liveIndexes = await liveCollection.indexes();
            // Retrieve indexes from the dev database
            const devIndexes = await devCollection.indexes();

            // Create a map for existing indexes in dev
            const devIndexMap = devIndexes.reduce((map, index) => {
                map[index.name] = index;
                return map;
            }, {});

            for (const index of liveIndexes) {
                if (index.name === '_id_') continue; // Skip the default _id index

                const { name, key } = index;

                // Check if index exists in dev
                const existingIndex = devIndexMap[name];

                if (existingIndex) {
                    // Compare index properties (you can expand this comparison as needed)
                    const isDifferent = existingIndex.key.toString() !== key.toString();

                    if (isDifferent) {
                        // Drop the existing index and create the updated one
                        console.log(`Dropping outdated index '${name}' from collection '${collectionName}'`);
                        await devCollection.dropIndex(name);
                        console.log(`Creating updated index '${name}' in collection '${collectionName}'`);
                        await devCollection.createIndex(key, { name });
                    }
                } else {
                    // Create new index
                    console.log(`Creating new index '${name}' in collection '${collectionName}'`);
                    await devCollection.createIndex(key, { name });
                }
            }
        }
    } catch (err) {
        console.error('Error copying and updating indexes:', err);
    } finally {
        await liveClient.close();
        await devClient.close();
    }
}


const liveUri = process.env.LIVE_DB_URL;
const devUri = process.env.DEV_DB_URL;

copyAndUpdateIndexes(liveUri, devUri);
