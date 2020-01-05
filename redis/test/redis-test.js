require("should")
	
const redis = require("../index")("test")

describe('redis', function () {
after(function () {
      redis.done()
    });
      
    context('service run is ok!', function () {
        it("#set()", async function () {

            let kvs = [{
                    "k5": "v1"
                },
                {
                    "k6": "v2"
                }
            ]
            let res = await redis.hset(kvs)
            console.log(JSON.stringify(res))

            kvs = {
                "k7": "v1",
                "k8": "v2"
            }

            res = await redis.hset(kvs)
            console.log(JSON.stringify(res))

            res.should.have.length(2)
        });

        it('#get', async function () {
            let res = await redis.hget()
            console.log(JSON.stringify(res))
            res = await redis.hget("k5")
            console.log(JSON.stringify(res))
            res = await redis.hget(["k5", "k6"])
            console.log(JSON.stringify(res))
            res = await redis.hget(["k5"])
            console.log(JSON.stringify(res))
        });

    });


});