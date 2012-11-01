/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 12-9-24
 * Time: 下午6:06

 * To change this template use File | Settings | File Templates.
 */

var program = require("commander")

program.version('0.0.1')
    .usage('[options]')
    .option('-c, --conf <string>', 'config file')
    .parse(process.argv);

if(program.conf==undefined){
    console.log("must input config")
    process.exit(1)
}

var conf = require("./conf/"+program.conf);
process.conf = conf;

var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});

var log = log4js.getLogger("user");
process.log = log

var user = require("./userservice").user

user.on("error",function(err){
    log.info("error:"+err)
})


user.on("close",function(err){
    log.info("close:"+err)
})

process.on('uncaughtException', function (exception) {
    //log.info(exception);
});

user.listen(conf.server.port)

log.info("start up")











