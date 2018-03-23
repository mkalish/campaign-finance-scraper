const CKAN = require('ckan');
const fetch = require('node-fetch');
const bluebird = require('bluebird');
const moment = require('moment-timezone');
const _ = require('lodash')

// const client = new CKAN.Client('http://nginx', '019fc88e-44fa-491c-a6ed-46a2d878ef8f');
const client = new CKAN.Client('http://data.codefordc.org', 'edf23d2e-486a-4434-9ea5-ca15f03bab7e');
const asyncClient = bluebird.promisifyAll(client);

// Get the curret max object id from CKAN [ X ]
// Batch the results from ARCGIS in batches of 50 [ X ]
// Query MAR for address details [ X ]
// Check that the rows are valid (what is valid???) [ X ]
// Write the raw row to CKAN [ X ]
// Write the row + address details from MAR to CKAN [ X ]
// Write valid rows + address details from MAR to CKAN [ X ]

const BATCH_SIZE = 50;
const NO_MAR_RECORD = 'noMarRecord';
// const RAW_RESOURCE = '2c53c464-0e18-4645-a516-913377d4aafc';
// const MAR_RESOURCE = '4ae77af8-9a3b-4edf-a246-3e9d1ea6af1e';
// const VALID_RESOURCE = '405c2957-83b6-42f2-9c44-912e6b62e7de';

const RAW_RESOURCE = '8b4c8253-7fa4-4345-8567-45c3295da6c8';
const MAR_RESOURCE = 'a2b60c48-13dd-46b2-ade0-5281d9d2786a';
const VALID_RESOURCE = '1fc04e5f-2feb-4401-a933-2b4fa214fc8d';

const standardizeCoord = (coord) => {
    return parseFloat(coord).toPrecision(8);
}

const validateRow = (row) => {
    if(row.marAddress == NO_MAR_RECORD) {
        return false;
    }

    const rowCoordinatesMatch = standardizeCoord(row.marAddress.XCOORD) === standardizeCoord(row.data.XCOORD) 
        && standardizeCoord(row.marAddress.YCOORD) === standardizeCoord(row.data.YCOORD);
    return (rowCoordinatesMatch || !row.data.ADDRESS.includes(', DC')) && row.data.ELECTIONYEAR && row.data.AMOUNT && row.data.CANDIDATENAME && row.data.COMMITTEENAME;
}

function pushRecords(records, resourceId) {
    if(records.length === 0) {
        return Promise.resolve(
            {
                result: {
                    records: []
                }
            }
        )
    }

    return client.actionAsync('datastore_create', {
        resource_id: resourceId,
        force: true,
        records
    })
    .catch(err => console.log(err));
}

async function scrapeNewRows() {
    const currentMaxObjectId = await asyncClient
        .actionAsync('datastore_search_sql', {sql: `SELECT max("OBJECTID") from "${RAW_RESOURCE}"`})
        .then(out => parseInt(out.result.records[0].max, 10));
    const maxId = currentMaxObjectId + BATCH_SIZE;
    const newRows = await fetch(`http://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Service_WebMercator/MapServer/34/query?where=objectid+<=+${maxId}+AND+objectid+>+${currentMaxObjectId}&outFields=*&f=json`)
        .then(res => res.json())
        .then(body => body.features)
        .catch((err) => {
            console.log(err);
        })
    if(newRows.length === 0) {
        return false;
    } else {
        const addresses = newRows.map(row => row.attributes.ADDRESS);
        const base64Addresses = new Buffer(addresses.join('|')).toString('base64');
        const marAddresses = await fetch('http://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocationBatch2',
                {
                    method: 'POST',
                    body: JSON.stringify({'f': 'json', 'addr_base64': base64Addresses, 'addr_separator': '|', chunkSequnce_separator: '~'}),
                    headers: { 'Content-Type': 'application/json' },
                }
            )
            .then(res => res.json())
            .then((raw) => {
                const results = raw.slice(1);
                return results.map(r => (r.returnDataset && !_.isEmpty(r.returnDataset)) ? r.returnDataset.Table1 : [ NO_MAR_RECORD ])
            })
            .catch((err) =>{
                console.log('Failed to get MAR results ', err);
            });

        const newsRowAndMarAddress = newRows.map((row, index) => {
            const marAddress = marAddresses[index];
            return {
                data: {
                    ...row.attributes,
                },
                marAddress: marAddress[0]
            }
        });

        const validRows = newsRowAndMarAddress.filter((row) => {
            return validateRow(row);
        });

        const validRowsPushed = await pushRecords(validRows.map(r => {
                const record = {
                    ...r.data,
                    DATEOFRECEIPT: moment(new Date(r.data.DATEOFRECEIPT), 'America/New_York'),
                    GIS_LAST_MOD_DTTM: moment(new Date(r.data.GIS_LAST_MOD_DTTM), 'America/New_York'),
                    ELECTIONYEAR: r.data.ELECTIONYEAR || -1,
                    AMOUNT: r.data.AMOUNT || -1,
                    ADDRESS_ID: r.data.ADDRESS_ID || -1,
                    XCOORD: r.data.XCOORD || -1,
                    YCOORD: r.data.YCOORD || -1,
                    LATITUDE: r.data.LATITUDE || -1,
                    LONGITUDE: r.data.LONGITUDE || -1,
                    MARID: r.marAddress.MARID,
                    ZIPCODE: r.marAddress.ZIPCODE,
                    VOTE_PRCNCT: r.marAddress.VOTE_PRCNCT,
                    ANC: r.marAddress.ANC,
                    WARD: r.marAddress.WARD,
                    HAS_CONDO_UNIT: r.marAddress.HAS_CONDO_UNIT,
                    HAS_RES_UNIT: r.marAddress.HAS_RES_UNIT,
                    RES_TYPE: r.marAddress.RES_TYPE,
                };
                return record;
            }),
            VALID_RESOURCE
        )
        .catch(err => {
            console.log(`Failed to push validRecords because ${err}`)
        });

        console.log(`Pushed ${validRowsPushed.result.records.length} raw rows to ckan`);
        
        const rawRecordsPushed = await pushRecords(newsRowAndMarAddress.map(r => r.data).map(r => {
            const record =  {
                    ...r,
                    DATEOFRECEIPT: moment(new Date(r.DATEOFRECEIPT), 'America/New_York'),
                    GIS_LAST_MOD_DTTM: moment(new Date(r.GIS_LAST_MOD_DTTM), 'America/New_York'),
                    ELECTIONYEAR: r.ELECTIONYEAR || -1,
                    AMOUNT: r.AMOUNT || -1,
                    ADDRESS_ID: r.ADDRESS_ID || -1,
                    XCOORD: r.XCOORD || -1,
                    YCOORD: r.YCOORD || -1,
                    LATITUDE: r.LATITUDE || -1,
                    LONGITUDE: r.LONGITUDE || -1
                }
                return record;
            }),
            RAW_RESOURCE
        )
        .catch(err => {
            console.log(`Failed to push rawRecords because ${err}`)
        });

        console.log(`Pushed ${rawRecordsPushed.result.records.length} raw rows to ckan`);

        const marRecordsPushed = await pushRecords(newsRowAndMarAddress.map(r => {
                const record = {
                    ...r.data,
                    DATEOFRECEIPT: moment(new Date(r.data.DATEOFRECEIPT), 'America/New_York'),
                    GIS_LAST_MOD_DTTM: moment(new Date(r.data.GIS_LAST_MOD_DTTM), 'America/New_York'),
                    ELECTIONYEAR: r.data.ELECTIONYEAR || -1,
                    AMOUNT: r.data.AMOUNT || -1,
                    ADDRESS_ID: r.data.ADDRESS_ID || -1,
                    XCOORD: r.data.XCOORD || -1,
                    YCOORD: r.data.YCOORD || -1,
                    LATITUDE: r.data.LATITUDE || -1,
                    LONGITUDE: r.data.LONGITUDE || -1,
                    MARID: r.marAddress.MARID,
                    ZIPCODE: r.marAddress.ZIPCODE,
                    VOTE_PRCNCT: r.marAddress.VOTE_PRCNCT,
                    ANC: r.marAddress.ANC,
                    WARD: r.marAddress.WARD,
                    HAS_CONDO_UNIT: r.marAddress.HAS_CONDO_UNIT,
                    HAS_RES_UNIT: r.marAddress.HAS_RES_UNIT,
                    RES_TYPE: r.marAddress.RES_TYPE,
                };
                return record;
            }),
            MAR_RESOURCE
        )
        .catch(err => {
            console.log(`Failed to push marRecrods because ${err}`)
        });


        console.log(`Pushed ${marRecordsPushed.result.records.length} raw rows to ckan`);

        console.log(`Processed ${newRows.length}`);
        return true;
    }
}

async function loopUntilDone() {
    let moreRows = true;
    while(moreRows) {
        const moreRows = await scrapeNewRows();
    }
    return "done";
}

loopUntilDone();

