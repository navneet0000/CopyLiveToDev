const mongoose = require('mongoose');
const { MongoClient } = require('mongodb');
require('dotenv').config();

// Replace with your actual MongoDB connection strings
const liveDBUrl = process.env.LIVE_DB_URL;
const devDBUrl = process.env.DEV_DB_URL;

const clientIDs = ['661df2932db600c8d39820df', '6523a66f7c0ce19984c24969'];

// Function to copy data
async function copyCollectionsWithClientID(clientIDs) {
    const liveClient = new MongoClient(liveDBUrl);
    const devClient = new MongoClient(devDBUrl);

    try {
        // Connect to both databases
        await liveClient.connect();
        await devClient.connect();

        const liveConnection = liveClient.db('SalesHive_Live');
        const devConnection = devClient.db('SalesHive_Dev');

        // Get list of all collections in the Live database
        const collections = await liveConnection.listCollections().toArray();

        for (const collectionInfo of collections) {
            const collectionName = collectionInfo.name;

            // Skip collections that start with "temp"
            if (collectionName.startsWith('temp')) {
                console.log(`Skipping collection: ${collectionName}`);
                continue;
            }
            console.log(`\n===============Processing ${collectionName}================`);

            // if (collectionName === "testcopy") {
            
            // Access the collection dynamically
            const liveCollection = liveConnection.collection(collectionName);

            // Process in batches of 1000 documents
            const batchSize = 1000;
            let skip = 0;

            while (true) {
                // const documents = await liveCollection.find({ ClientID: { $in: clientIDs.map(id => new mongoose.Types.ObjectId(id)) } }).toArray();
                const documents = await liveCollection.find({ ClientID: { $in: clientIDs.map(id => new mongoose.Types.ObjectId(id)) } }).skip(skip).limit(batchSize).toArray();

                if (documents.length === 0) {
                    break;
                }

                // Check if the collection exists in the Dev database
                const devCollectionExists = await devConnection.listCollections({ name: collectionName }).hasNext();
                if (!devCollectionExists) {
                    console.log(`Creating collection: ${collectionName} in Dev database.`);
                    await devConnection.createCollection(collectionName);
                }
                const devCollection = devConnection.collection(collectionName);

                if (documents.length > 0) {
                    // Prepare bulk operations
                    const bulkOps = documents.map(doc => ({
                        updateOne: {
                            filter: { _id: doc._id },
                            update: { $set: doc },
                            upsert: true // Insert the document if it does not exist
                        }
                    }));

                    // Perform bulk write operation in the Dev database
                    const result = await devCollection.bulkWrite(bulkOps);
                    console.log(`${result.upsertedCount} documents inserted, ${result.modifiedCount} documents updated in ${collectionName}.`);

                    // Insert data into the corresponding Dev database collection
                    // await devCollection.insertMany(documents);
                    console.log(`${documents.length} documents copied from ${collectionName}.`);
                } else {
                    console.log(`No documents found in ${collectionName}.`);
                }
                // }

                skip += batchSize;
            }
            console.log(`===============END Processing================ \n`);
        }
    } catch (error) {
        console.error('Error copying data:', error);
    } finally {
        // Close connections
        await liveClient.close();
        await devClient.close();
    }
}

copyCollectionsWithClientID(clientIDs);
