// Explicit setup only. No fixture writes, data deletion or source adoption.
function worked(result) {
    if (result.ok !== 1) throw new Error('Mongo setup command failed');
}
const admin = db.getSiblingDB('admin');
try {
    const config = rs.conf();
    if (config._id !== 'task3' || config.members.length !== 1 || config.members[0].host !== 'mongo:27017') {
        throw new Error('Unexpected replica-set configuration; repair explicitly');
    }
} catch (error) {
    if (error.code !== 94) throw error;
    rs.initiate({_id: 'task3', members: [{_id: 0, host: 'mongo:27017'}]});
}
const deadline = Date.now() + 30000;
while (!admin.runCommand({hello: 1}).isWritablePrimary) {
    if (Date.now() >= deadline) throw new Error('Primary election timed out');
    sleep(100);
}
const source = db.getSiblingDB('task3');
for (const name of ['clients', 'orders', 'restaurant']) {
    const info = source.getCollectionInfos({name: name});
    if (!info.length) {
        worked(source.createCollection(name, {changeStreamPreAndPostImages: {enabled: true}}));
    } else {
        if (info[0].type !== 'collection') throw new Error('Expected collection: ' + name);
        worked(source.runCommand({collMod: name, changeStreamPreAndPostImages: {enabled: true}}));
    }
}
print('task3 replica set and image options ready; existing documents retained');
