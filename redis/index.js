require('dotenv').config()
const _ = require("lodash"),
    fs = require('@cnbany/fs'),
    Redis = require("ioredis"),
    log = require("debug")("@cnbany-redis:")

let redisOpt = {
    "host": process.env.REDIS_HOST || "127.0.0.1",
    "port": process.env.REDIS_PORT || 6379,
    "password": process.env.REDIS_PASS || "",
    "family": 4
}

//返回json对象
function _json(ret) {

    if (_.isArray(ret) || _.isObject(ret))
        for (let i in ret)
            ret[i] = (ret[i]) ? JSON.parse(ret[i]) : {}

    else if (_.isString(ret))
        ret = (ret) ? JSON.parse(ret) : {}

    return ret
}

async function _hget(ins, name, keys, json) {
    let ret
    // keys 为 all 时，获取所有信息
    if (_.isEmpty(keys))
        ret = {}
    else if (keys == "all")
        ret = await ins.hgetall(name)
    else if (Array.isArray(keys) && keys.length > 0) {
        ret = await ins.hmget(name, ...keys)
    } else if (typeof (keys) == 'string') {
        ret = await ins.hget(name, keys)
    }

    if (json && ret != {}) ret = _json(ret)

    return ret
}


async function _hset(ins, name, kvs) {

    let pipeline = ins.pipeline();

    if (!kvs || (Array.isArray(kvs) && kvs.length == 0))
        return []

    else if (Array.isArray(kvs) && kvs.length > 0) {

        for (let i in kvs)
            if (Object.keys(kvs[i]).length > 0)
                for (let key in kvs[i])
                    pipeline.hset(name, key, kvs[i][key])

    } else if (Object.keys(kvs).length > 0) {

        for (let key in kvs)
            pipeline.hset(name, key, kvs[key])
    }

    let ret = await pipeline.exec();

    //处理返回结果 [[nil,1],[nil,1]] => [1,1]
    for (let i in ret) {
        if (Array.isArray(ret[i]) && ret[i].length == 2)
            ret[i] = ret[i][1]
    }

    return ret
}


function redis(name = index, opt) {

    let json = (opt == "json") ? true : false,
        ins = new Redis(redisOpt)

    let obj = {

        done: () => {
            ins.disconnect()
            log(`redis [${name}] is disconnected `)
        },

        get: async (key) => {
            log(`get: is begin. get `, key)
            let ret = await ins.get(key)
            log(`get: is success done. get `, key)
            return ret
        },

        set: async (key, value) => {
            log(`set: is begin. set `, key)
            let ret = await ins.set(key, value)
            log(`set: is success done. set `, key)
            return ret
        },

        hget: async (keys) => {
            log(`hget: is begin. hget [${name}] `, keys || "all")
            let ret = await _hget(ins, name, keys, json)
            log(`hget: is success done ! hget [${name}] `)
            return ret
        },

        hset: async (kvs) => {
            log(`hset: is begin. hset [${name}] `)
            let ret = await _hset(ins, name, kvs)
            log(`hset: is success done! hset [${name}] `)
            return ret
        },

        hkeys: async () => {
            log(`hkeys: is begin. hkeys [${name}] `)
            return await ins.hkeys(name)
        },

        hdump: async (file) => {

            file = file || `${name}.all.ndjson`
            log(`hdump: is begin. dump [${name}] to file [${file}]`)

            let opt = 'w',
                ids = await ins.hkeys(name),
                chunks = _.chunk(ids, 10000)

            for (let i in chunks) {
                let res = await _hget(ins, name, chunks[i], json)
                fs.write(file, res, opt, "ndjson")
                if (opt == 'w') opt = 'a'
            }
            log(`hdump: is done ! dump [${name}] to file [${file}]`)
        },
    }
    return obj
}

module.exports = redis;