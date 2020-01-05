// require("array.prototype.flatmap").shim();
// const dsl = require("bodybuilder");
/* 
API Referenceedit
https://bodybuilder.js.org/docs/
https://bodybuilder.js.org/
 */

const _ = require("lodash"),
    fs = require("@cnbany/fs"),
    log = require("debug")("@cnbany-elastic:"),
    _parse = require("./lib/parse"),
    _request = require("./lib/request")


const EventEmitter = require('events').EventEmitter


class es extends EventEmitter {
    constructor(index = "index", key = "id") {
        super()
        this.index = index;
        this.key = key;
        this.scroll_id = "";
        this.search_count = 0;
    }
}

es.prototype.search = async function (qs) {

    if (!qs) return null
    log(qs);

    let res = []

    let body = await _request.search(this.index, qs)

    this.search_count = body.hits.total.value;
    this.scroll_id = body._scroll_id;

    let ss = _parse.search(body);
    this.emit("data", ss)
    res.push(...ss)

    while (Array.isArray(ss) && ss.length > 0 && res.length < this.search_count) {

        body = await _request.scroll(this.scroll_id)

        if (body) {
            ss = _parse.search(body)
            this.emit("data", ss)
            res.push(...ss)
        } else ss = []
    }

    this.emit("searchdone")
    return res
};

es.prototype.upsert = async function (id, ob) {

    let body = await _request.upsert(this.index, id, ob)

    if (body._shards.successful)
        log(`DB ["${body._index}"] ID ["${body._id}"] ${body.result} success!`)
    else
        log(`DB ["${body._index}"] ID ["${body._id}"] ${body.result} failed!`)

    this.emit("postdone")

};

es.prototype.crwdone = async function (id, cls) {
    let crw = {
        "cls": new Date().valueOf()
    }

    await _request.upsert(this.index, id, {
        crw
    })
}

es.prototype.bulk = async function (obs, key = "id") {

    let obsChunk = _.chunk(obs, 1000);
    for (let i in obsChunk) {

        let body = await _request.bulk(this.index, obsChunk[i], key)
        // result process
        // let ret = _parse.bulk(body)
        log(i * 1000 + obsChunk[i].length)
    }
    await _request.refresh(this.index)
    this.emit("postdone")
}


es.prototype.count = async function () {
    return await _request.count(this.index)
}

es.prototype.import = function (file, idkey) {

    if (!file || !fs.exist(file)) return false

    let that = this,
        datas = []

    lineFn = (line) => {
        datas.push(JSON.parse(line))
    }

    doneFn = async () => {
        await that.bulk(datas, idkey)
    }

    fs.readline(file, lineFn, doneFn)
}

es.prototype.dump2redis = async function (qs, redis) {

    if (!qs) return null
    log(qs);

    let res = []

    let body = await _request.search(this.index, qs)

    this.search_count = body.hits.total.value;
    this.scroll_id = body._scroll_id;

    let ss = _parse.search(body);
    ss = _.reduce(ss, function (result, item) {
        let o = {}
        o[item.id] = JSON.stringify(item)
        result.push(o)
        return result;
    }, [])

    await redis.set(ss)

    while (Array.isArray(ss) && ss.length > 0 && res.length < this.search_count) {

        body = await _request.scroll(this.scroll_id)

        if (body) {
            ss = []
            ss = _parse.search(body)
            ss = _.reduce(ss, function (result, item) {
                let o = {}
                o[item.id] = JSON.stringify(item)
                result.push(o)
                return result;
            }, [])

            await redis.set(ss)
        } else ss = []
    }

}

module.exports = es;