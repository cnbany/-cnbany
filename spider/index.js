/*
on(start)
 on(next)
    ->search()
        -> on("get") 
            -> Spider()
                -> on("set") 
                    -> update()/upsert() 
                        -> on("next")

*/

const _ = require("lodash"),
    log = require("debug")("@cnbany-spider:"),
    agents = require("./lib/agents"),
    Crawler = require("crawler"),
    Elastic = require("@cnbany/elastic"),
    EventEmitter = require('events').EventEmitter

let bnext = true,
    crawled = []


async function done(spider) {
    log("begin next !!!!!!!!!!!!!")
    // await shell.ipflush()
    let ret = await spider.db.scroll()
    if (ret <= 0 && bnext) {
        log("next search is begin!");
        spider.emit("next", spider)
        bnext = false
    }
}

class Spider extends EventEmitter {
    constructor(db = "index") {
        super()
        this.db = new Elastic(db);

        this.ce = new Crawler({
            maxConnections: 10,
            rateLimit: 100,
            jQuery: false,
            headers: {
                "content-type": "application/json",
                "User-Agent": agents("pc")
            },
            callback: (error, res, done) => {
                log(`Spider page url: ${res.options.url} is done!`);
                if (error) throw error;
                if (!_.startsWith(res.statusCode, "2"))
                    throw {
                        message: res.statusMessage,
                        code: res.statusCode
                    };
                this.emit("set", res)
                done();
            }
        });


        //event transfrom

        this.db.on("data", (res) => {
            for (let i in res)
                this.emit("get", res[i])
        });

        this.db.on("postdone", () => {
            if (this.ce.queueSize == 0) done(this)
        });

    }
}



Spider.prototype.search = function (qs) {
    this.db.search(qs, true)
}

Spider.prototype.fetch = function (opt) {
    this.ce.queue(opt)
}

Spider.prototype.update = function (result, flag) {

    if (!_.isArray(result) || result.length == 0) return
    this.db.bulk(result)
        .then(() => {
            bnext = true

            if (flag && result[0].frm && result[0].frm.id) {
                this.db.crwdone(result[0].frm.id, flag)
            }
        })
        .catch(onFailed);
};

Spider.prototype.upsert = function (result, mode = "done") {
    if (JSON.stringify(result) === "{}") return
    this.db.upsert(result.id, result)
        .then(() => {
            bnext = true
            if (result.frm.id) {
                this.db.crwdone(result.frm.id, mode)
            }
        })
        .catch(onFailed);
};

Spider.prototype.run = async function () {
    let count = await this.db.count()
    if (count > 0) {
        this.emit("next")
    } else {
        this.emit("start")
    }
}

module.exports = Spider