const MongoClient = require("mongodb").MongoClient;
const v8Profiler = require("v8-profiler-next");
const fs = require("fs");

const DRIVER_VERSION = "4.6.0";
const DATABASE_NAME = "loadtest";
const COLLECTION_NAME = "test";
const DOCS_TO_INSERT_COUNT = 10 * 1000;
const ARRAY_FIELD_SIZE = 20;
const REQUESTS_COUNT = 300;
const PROFILE_CPU = true;
const BENCHMARK_ITERATIONS_COUNT = PROFILE_CPU ? 1 : 3;

const url = "mongodb://localhost:27017";
const client = new MongoClient(url, {
  enableUtf8Validation: false,
  ignoreUndefined: true,
});

let db;

async function main() {
  await client.connect();
  db = client.db(DATABASE_NAME);
  await maybePopulateDb();

  const tag = `Fetch ${DOCS_TO_INSERT_COUNT} docs ${REQUESTS_COUNT} times in parallel (driver ${DRIVER_VERSION})`;

  for (let i = 0; i < BENCHMARK_ITERATIONS_COUNT; i += 1) {
    console.time(tag);

    profileCpu({ duration: 60 * 1000 });
    await benchmark();

    console.timeEnd(tag);
    await sleep(10);
  }
}

main()
  .catch(console.error)
  .finally(() => client.close());

async function maybePopulateDb() {
  const insertedDocsCount = await getInsertedDocsCount();
  if (!insertedDocsCount) {
    await populateDb();
  } else if (DOCS_TO_INSERT_COUNT !== insertedDocsCount) {
    await clearDb();
    await populateDb();
  }
}

function getInsertedDocsCount() {
  return db.collection(COLLECTION_NAME).countDocuments({});
}

function clearDb() {
  return db.collection(COLLECTION_NAME).drop();
}

async function populateDb() {
  const docs = composeDocs();
  await bulkInsertDocs(docs);
}

function composeDocs() {
  const docs = new Array(DOCS_TO_INSERT_COUNT);
  const arrayField = composeArrayField();

  for (let i = 0; i < DOCS_TO_INSERT_COUNT; i += 1) {
    docs[i] = { _id: i, arrayField };
  }

  return docs;
}

function composeArrayField() {
  const arrayField = new Array(ARRAY_FIELD_SIZE);

  for (let i = 0; i < ARRAY_FIELD_SIZE; i += 1) {
    arrayField[i] = "5e99f3f5d3ab06936d360" + i;
  }

  return arrayField;
}

async function bulkInsertDocs(docs) {
  const operations = new Array(docs.length);

  for (let i = 0; i < docs.length; i += 1) {
    operations[i] = { insertOne: docs[i] };
  }

  await db.collection(COLLECTION_NAME).bulkWrite(operations);
}

async function benchmark() {
  const promises = new Array(REQUESTS_COUNT);

  for (let i = 0; i < REQUESTS_COUNT; i += 1) {
    promises[i] = fetchDocs();
  }

  return Promise.all(promises);
}

function fetchDocs() {
  return db.collection(COLLECTION_NAME).find({}).toArray();
}

function sleep(seconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000);
  });
}

function profileCpu({ duration }) {
  const cpuProfileTitle = `driver-${DRIVER_VERSION}-fetch-${DOCS_TO_INSERT_COUNT}-docs-${REQUESTS_COUNT}-times-${Date.now()}`;

  v8Profiler.setGenerateType(1);
  v8Profiler.setSamplingInterval(1000);
  v8Profiler.startProfiling(cpuProfileTitle, true);

  setTimeout(() => {
    const profile = v8Profiler.stopProfiling(cpuProfileTitle);
    profile.export(function (error, result) {
      fs.writeFileSync(`${cpuProfileTitle}.cpuprofile`, result);
      profile.delete();
    });
  }, duration);
}
