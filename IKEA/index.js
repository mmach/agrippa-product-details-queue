
var Crawler = require("crawler");
var parseString = require('xml2js').parseString;
const amqp = require('amqplib/callback_api');



const CONN_URL = process.env.AMQP ? process.env.AMQP : 'amqp://oqxnmzzs:hUxy1BVED5mg9xWl8lvoxw3VAmKBOn7O@squid.rmq.cloudamqp.com/oqxnmzzs';

let add_to_to_queue = (obj, resolve) => {

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

        skipDuplicates: true,
        // This will be called for each crawled page
        preRequest: function (options, done) {
            done();

        },
        callback: function (error, res, done) {
            if (error) {
                console.log("#############ERROR############")
                console.log(error);
                setTimeout(() => {

                    done();
                }, 60000)
            } else {
                var $ = res.$;
                //  console.log(res.body)
                //      let name = $('.product-compact__name').children[0].data;
                let prod = {};
                try {
                    let name = $('.product-compact__name')["0"].children[0].data;
                    let external_id = $('.product-compact')[0].attribs["data-ref-id"]
                    let price = $('.product-compact')[0].attribs["data-price"]
                    let currency = $('.product-compact')[0].attribs["data-currency"]
                    let status = $('.product-compact')[0].attribs["data-online-sellable"]
                    prod = {
                        source: 'IKEA',
                        external_id: external_id,
                        group: obj.title,
                        link: obj.href,
                        product: name,
                        price: price,
                        currency: currency,
                        status:status=='true'?'online-sellable':''

                    }
                }
                catch (err) {
                    console.log(err);
                    done();
                    resolve();
                }

                //     console.log(productsList);


                var client = null;
                amqp.connect(CONN_URL, function (err, conn) {
                    if (err) {
                        console.log("CONNECTION ERROR");
                        console.log(err);
                        setTimeout(() => {
                            done();
                            resolve();
                            conn.close();

                        }, 60000)
                        return;
                    }
                    conn.createChannel(async function (err2, channel) {
                        if (err2) {
                            console.log("CHANEL ERROR");

                            console.log(err);
                            setTimeout(() => {
                                resolve()
                                done();
                            }, 60000)
                            return

                        } ch = channel;
                        channel.assertQueue('product-item-queue', {
                            durable: true
                        });


                        ch.sendToQueue('product-item-queue', new Buffer(JSON.stringify(prod)), { persistent: true });
                        setTimeout(() => {
                            channel.close();
                            conn.close();
                            done();
                            resolve();
                        }, 500)

                    });
                })

                //console.log(obj.href)
                //console.log(nextPage);



                done()

            }
        }
    }

    );
}

module.exports = {
    add_to_to_queue
}

    // Queue some HTML code directly without grabbing (mostly for tests)

