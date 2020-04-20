var amqp = require('amqplib/callback_api');
var Crawler = require("crawler");
const redis = require('async-redis');
var Promise = require('bluebird');
const CONN_URL = process.env.AMQP ? process.env.AMQP : 'amqp://oqxnmzzs:hUxy1BVED5mg9xWl8lvoxw3VAmKBOn7O@squid.rmq.cloudamqp.com/oqxnmzzs';
const PREFETCH = process.env.PREFETCH ? process.env.PREFETCH : 5;
const MAX_CONNECTIONS = process.env.MAX_CONNECTIONS ? process.env.MAX_CONNECTIONS : 10;
const obs = require('./OBS/index.js')
global.c = new Crawler({
    maxConnections: MAX_CONNECTIONS,
    retries: 30,
    retryTimeout: 60000,

});



// Queue just one URL, with default callback


// Queue some HTML code directly without grabbing (mostly for tests)
amqp.connect(CONN_URL, async function (error0, connection) {
    if (error0) {
        console.log(error0)
        throw error0;
    }

    connection.createChannel(function (error1, channel) {

        if (error1) {
            throw error1;
        }

        var queue = 'product-item-queue';

        channel.prefetch(PREFETCH);

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
        channel.consume(queue, async function (msg) {
            let arrayProductsGroup = []
            let arrayProduct = []
            let obj = msg.content.toString();
            obj = JSON.parse(obj);
            try {

                await new Promise((resolve, reject) => {
                    if (obj.source == 'HAGELAND.NO') {
                        arrayProduct.push(obj);
                        resolve();
                    }
                    else if (obj.source == 'BLOMSTERLANDET.SE') {
                        arrayProduct.push(obj);
                        resolve();
                    }
                    else if (obj.source == 'OBS') {
                        global.obs_crawler.queue({
                            uri: obj.href,
                            forceUTF8: false,
                            headers: {
                                "Content-Type": "application/json",
                                "sec-fetch-site": "same-origin",
                                "sec-fetch-mode": "navigate",
                                "sec-fetch-user": "?1",
                                "upgrade-insecure-requests": 1,
                                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3"


                            },
                            skipDuplicates: true

                        })
                        // arrayProduct.push(obj);
                        resolve();
                    } else if (['OBS_BYGG', 'OBS_SORTIMENT'].includes(obj.source)) {
                        arrayProduct.push(obj);
                        resolve();
                    }
                })


                await new Promise((resolve, reject) => {
                    amqp.connect(CONN_URL, function (errItem, connItem) {
                        if (errItem) {
                            console.log("CONNECTION ERROR");
                            console.log(errItem);
                            reject();
                            return;
                        }
                        connItem.createChannel(async function (err2, channelItem) {
                            if (err2) {
                                console.log("CHANEL ERROR");
                                channelItem.close();
                                connItem.close();
                                reject();

                                console.log(err);

                                return

                            } chItem = channelItem;
                            channelItem.assertQueue('ready-to-import-queue', {
                                durable: true
                            });

                            let promises = arrayProduct.map(item => {
                                return chItem.sendToQueue('ready-to-import-queue', new Buffer(JSON.stringify(item)), { persistent: true });
                            })
                            await Promise.all(promises)

                            setTimeout(() => {
                                channelItem.close();
                                connItem.close();
                                resolve();

                                //  ch.close();
                            }, 500)

                        });
                    })
                });
                console.log(obj)
                channel.ack(msg)
            } catch (err) {
                console.log(err);
                setTimeout(() => {
                    channel.nack(msg)

                }, 60000)

            }

            // console.log(arrayProduct);
            // channel.nack(msg);
            // console.log(" [x] Received %s", msg.content.toString());

        }, {
            noAck: false
        });
    });
});
