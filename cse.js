'use strict'

require('env-yaml').config()
const { BigQuery } = require('@google-cloud/bigquery')
const fs = require('fs-extra')
const LRU = require('quick-lru')
const SC = require('@google-cloud/security-center').v1beta1
const securityCenter = new SC.SecurityCenterClient({
  projectId: process.env.PROJECT_ID,
  keyFilename: process.env.CREDENTIALS
})

const { info, success, err } = require('./logger')
const fields = require('./static/fields.json')

const lru = new LRU({ maxSize: 200 })

const cacheHandler = {
  get (target, prop, receiver) {
    return Reflect.get(...arguments)
  },
  set (obj, prop, value) {
    return Reflect.set(...arguments)
  }
}

const Cache = {
  colos: new Proxy(lru, cacheHandler),
  assets: new Proxy(lru, cacheHandler),
  get rationales () {
    let outcomes = fields
    outcomes = Array.from([outcomes.EdgePathingStatus, outcomes.EdgePathingSrc])
    outcomes = new Map(Object.entries(outcomes[0]))
    return outcomes
  }
}

class CSE {
  constructor ({ orgPath, source }) {
    this.orgPath = orgPath
    this.source = source
    this._assets = [this.orgPath]
    this.finding = {}
    this.rationale = Cache.rationales
  }

  get assets () {
    return this._assets
  }

  set assets (asset) {
    if (asset.length > 3) this._assets.push(asset)
  }

  listFindings () {
    let $this = this
    securityCenter.listFindingsStream({ parent: this.source })
      .on('data', element => {
        info(JSON.stringify(element, null, 2))
        console.log(this)
        return $this
      }).on('error', err => {
        console.log(err)
      })
  }

  formatFinding (log) {
    this.assets = log.OriginIP
    console.log(log)

    this.finding = {
      name: `${this.source}/findings/${log.RayID}`,
      externalUri: `https://dash.cloudflare.com/`,
      state: 'ACTIVE',
      resourceName: this.assets[0],
      sourceProperties: {
        Action: {
          stringValue: log.EdgePathingStatus,
          kind: 'stringValue'
        },
        Status: {
          stringValue: `${log.EdgeResponseStatus}`,
          kind: 'stringValue'
        },
        Host: {
          stringValue: log.ClientRequestHost,
          kind: 'stringValue'
        },
        URI: {
          stringValue: `${log.ClientRequestMethod} ${log.ClientRequestURI}`,
          kind: 'stringValue'
        },
        Country: {
          stringValue: log.ClientCountry.toUpperCase(),
          kind: 'stringValue'
        },
        Location: {
          stringValue: CSE.getColo(log.EdgeColoID),
          kind: 'stringValue'
        },
        ClientIP: {
          stringValue: log.ClientIP,
          kind: 'stringValue'
        },
        ClientASN: {
          stringValue: log.ClientASN,
          kind: 'stringValue'
        },
        Device: {
          stringValue: log.ClientDeviceType,
          kind: 'stringValue'
        },
        EdgePathingSignature: {
          stringValue: `${log.EdgePathingStatus} ${log.EdgePathingSrc}`
        },
        ClientRequestBytes: {
          stringValue: log.ClientRequestBytes,
          kind: 'stringValue'
        },
        ClientSSLCipher: {
          stringValue: log.ClientSSLCipher,
          kind: 'stringValue'
        },
        UA: {
          stringValue: log.ClientRequestUserAgent,
          kind: 'stringValue'
        },
        Referer: {
          stringValue: log.ClientRequestReferer,
          kind: 'stringValue'
        }
      },
      securityMarks: {
        OriginIP: log.OriginIP
      }
    }

    switch (true) {
      case log.EdgeResponseStatus === 429:
        this.finding.category = 'Block: Rate Limit'

        break

      case log.EdgePathingSrc === 'filterBasedFirewall':
        this.finding.category = `Firewall Rules: ${fields.EdgePathingStatus[log.EdgePathingStatus]}`
        break

      case log.WAFRuleMessage.length > 2:
        this.finding.category = log.WAFRuleMessage
        this.finding.sourceProperties.WAFAction = log.WAFAction
        this.finding.sourceProperties.WAFProfile = log.WAFProfile
        this.finding.sourceProperties.Action = {
          stringValue: log.WAFAction,
          kind: 'stringValue'
        }
        break

      default:
        const rationales = [log.EdgePathingStatus, log.EdgePathingSrc]
        let i = 0
        while (i < rationales.length) {
          const ratch = rationales[i]
          if (this.rationale.has(ratch)) {
            this.finding.category = this.rationale.get(ratch)
            break
          }
          i++
        }
    }

    if (this.finding.category.length < 3) {
      this.finding.category = `Cloudflare Firewall Event`
    }

    if (this.finding.category === undefined) {
      this.finding.category = `Cloudflare Firewall Event`
    }

    if (this.finding.resourceName.length < 3) {
      this.finding.resourceName = this.orgPath
    }

    if (this.finding.resourceName === undefined) {
      this.finding.resourceName = this.orgPath
    }

    // let eventTime = Date.parse(log.EdgeStartTimestamp.value)
    // console.log(eventTime.toString().slice(0, 10))

    this.finding.eventTime = {
      seconds: Number.parseInt(`${Date.now().toString().slice(0, 10)}`),
      nanos: Number.parseInt(`${Date.now().toString().slice(0, 9)}`)
    }

    return this
  }

  // Map EdgeColoID to the city where the colo resides
  static getColo (edgeColoID) {
    let id = Number.parseInt(edgeColoID, 10)
    let colos = require('./static/colos.json')
    let inCache = Cache.colos.has(edgeColoID)

    if (!inCache) {
      if (edgeColoID <= 172) {
        Cache.colos.set(edgeColoID, String(colos[id].colo_alias))
        return Cache.colos.get(edgeColoID)
      }

      const inChina = colos.slice(172).findIndex(colo => colo.colo_id === edgeColoID)

      if (inChina > -1) {
        Cache.colos.set(edgeColoID, String(colos[id].colo_alias))
        return Cache.colos.get(edgeColoID)
      }

      return 'San Francisco, CA'
    }
    return Cache.colos.get(edgeColoID)
  }

  update () {
    let fieldMask = {
      mask: 'attribute.sourceProperties,attribute.resourceName,attribute.eventTime,attribute.securityMarks'
    }
    info('update() called')
    console.log(this.finding)
    securityCenter.updateFinding({
      finding: this.finding
    }).then(responses => {
      const outcome = responses[0].name
      success(outcome)
      // success(this.finding.name)
      return outcome
    }).catch(e => {
      err(e)
    })
    return this.done()
  }

  done (prom) {
    const $that = this
    let done
    try {
      (async (done = false) => {
        await prom
        return done
      })(done)
    } catch (e) {
      err(e)
    } finally {
      done = true
    }
    if (done) return $that
    // row is a result from your query.
  }

  async addFindings ({ queries = ['./static/queries/threats.txt'] }) {
    const $that = this
    const bigquery = new BigQuery()

    const runQueries = queries.map(async qry => {
      qry = fs.readFileSync(qry)
      qry = `${qry}`.replace('BQ_DATASET', process.env.BQ_DATASET)
      info(`Running query: ${qry}`)

      await bigquery.createQueryStream(qry)
        .on('error', console.error)
        .on('data', (row) => { $that.formatFinding(row).update() })
        .on('end', () => { success('Waiting on response from SCC ...') })
    })

    // log them in sequence
    for (const runQuery of runQueries) {
      console.log(await runQuery)
    }
  }

  assetsStream () {
    console.log(Cache.assets.keys())
    securityCenter.listAssetsStream({
      parent: this.orgPath,
      filter: `securityCenterProperties.resourceType="google.compute.Address"`
    })
      .on('data', elem => {
        Cache.assets.set(elem.asset.securityCenterProperties.resourceName, elem.asset.resourceProperties.address.stringValue)
        console.log(Cache.assets.get(elem.asset.securityCenterProperties.resourceName))
        // doThingsWith(element)
      }).on('error', err => {
        console.log(err)
      })
  }

  static async init () {
    let orgPath = `organizations/${process.env.GCLOUD_ORG}`
    let sources = await securityCenter.listSources({
      parent: orgPath
    })
    sources = sources[0]

    let getSource = sources.filter(src => src.displayName === 'Cloudflare')
    if (getSource[0] !== 'Cloudflare') {
      getSource = await securityCenter.createSource({
        parent: orgPath,
        source: {
          displayName: 'Cloudflare'
        }
      })
      sources = sources[0]
    } else {
      sources = getSource[0]
    }

    info(`Using source ${sources.displayName} in o${orgPath}`)
    return new CSE({
      orgPath: orgPath,
      source: sources.name
    })
  }
}

module.exports = CSE
