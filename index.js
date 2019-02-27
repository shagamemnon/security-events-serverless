'use strict'

require('env-yaml').config()
const { BigQuery } = require('@google-cloud/bigquery')
const { Storage } = require('@google-cloud/storage')
const queries = ['./static/queries/no_waf.txt', './static/queries/waf.txt', './static/queries/rate_limit.txt']
const _schema = require('./static/schema.json')
const CSE = require('./cse')

module.exports.gcsbq = async function (file, context) {
  const datasetId = (process.env.BQ_DATASET).split('.')[0]
  const tableId = (process.env.BQ_DATASET).split('.')[1]

  const bigquery = new BigQuery()

  const storage = new Storage()

  console.log(`Starting job for ${file.name}`)

  const filename = storage.bucket(file.bucket).file(file.name)

  /* Configure the load job and ignore values undefined in schema */
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    schema: {
      fields: _schema
    },
    ignoreUnknownValues: true
  }

  const dataset = bigquery.dataset(datasetId)

  await dataset.get({ autoCreate: true }, (e, dataset, res) => {
    if (e) console.log(e)
    dataset.table(tableId).get({ autoCreate: true }, (e, table, res) => {
      table.load(filename, metadata)
    })
  })
}

module.exports.bqscc = async function (data, context) {
  const cse = await CSE.init()
  await cse.addFindings({
    queries: queries
  })
}
