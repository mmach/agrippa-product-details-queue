
var Crawler = require("crawler");
var parseString = require('xml2js').parseString;
const amqp = require('amqplib/callback_api');



const CONN_URL = process.env.AMQP ? process.env.AMQP : 'amqp://oqxnmzzs:hUxy1BVED5mg9xWl8lvoxw3VAmKBOn7O@squid.rmq.cloudamqp.com/oqxnmzzs';

let add_to_to_queue = (href, resolve) => {

    global.obs_crawler.queue({
        uri: href,
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
                let productsList = $('[type="application/ld+json"]');
                //     console.log(productsList);
                let group = {}

                let products = []
                Object.keys(productsList).filter(item => {
                    return isNaN(item) == false
                }).map(item => {
                    if (productsList[item].children[0].data.includes(`"@type":"Product"`)) {
                        let obj = JSON.parse(productsList[item].children[0].data)
                        obj.offers.offers.forEach(offer => {
                            products.push(
                                {
                                    source: 'OBJ_PROCESSED',
                                    external_id: obj.gtin13 + "-" + offer.variantCode,
                                    //group: obj.title,
                                    link: offer.url,
                                    product: obj.name,
                                    price: offer.price,
                                    currency: offer.priceCurrency,
                                    // prom: prom,
                                    status: offer.availability.split('/')[3],
                                    //  subgroup: subgroup,
                                    name_lng: obj.description,
                                    image: obj.image,
                                    GTIN: obj.gtin13

                                });
                        })

                    } else if (productsList[item].children[0].data.includes(`"@type":"BreadcrumbList"`)) {
                        group = JSON.parse(productsList[item].children[0].data);
                    }
                });
                products = products.map(item => {

                    item.source = group.itemListElement[0].item["@id"] == 'http://coop.no/obsbygg/' ? 'OBS_BYGG' : 'OBS_SORTIMENT'
                    item.group = group.itemListElement[group.itemListElement.length - 1].item.name
                    return item;
                })


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

                        products.forEach(item => {
                            ch.sendToQueue('product-item-queue', new Buffer(JSON.stringify(item)), { persistent: true });
                        })
                        setTimeout(() => {
                            done();
                            resolve();
                        }, 500)

                    });
                })
                console.log(products);

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

