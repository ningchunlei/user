var thrift = require("thrift")
var util = require("util");
var mysql = require("mysql-native")

var UserService = require("./thrift/UserIFace")
var ShareStruct_ttypes = require("./thrift/ShareStruct_Types")
var ErrorNo_ttypes = require("./thrift/ErrorNo_Types")
var Exception_ttypes = require("./thrift/Exception_Types")
var redis = require("redis")

var poolModule = require('generic-pool');
var pool = poolModule.Pool({
    name     : 'redis',
    create   : function(callback) {
        var client = redis.createClient(process.conf.redis.port,process.conf.redis.ip);
        client.auth(process.conf.redis.passwd)
        callback(null, client);
    },
    destroy  : function(client) { client.quit(); }, //当超时则释放连接
    max      : 10,   //最大连接数
    idleTimeoutMillis : 10,  //超时时间
    log : true
});

var mysqlPool = poolModule.Pool({
    name     : 'mysql',
    create   : function(callback) {
        var client = mysql.createTCPClient(process.conf.mysql.ip,process.conf.mysql.port);
        client.auth(process.conf.mysql.u,process.conf.mysql.p,process.conf.mysql.db)
        callback(null, client);
    },
    destroy  : function(client) { client.close() }, //当超时则释放连接
    max      : 10,   //最大连接数
    idleTimeoutMillis : 10,  //超时时间
    log : true
});

var FALSE = -1
var TRUE = 1

var server = exports.timeline = thrift.createServer(UserService,{
    isRegister:function(phone,response){
        mysqlPool.borrow(function(err,mysql){
            ret = FALSE;
            mysql.execute("select phone from user where phone=?",[phone]).on("row",function(r){
                ret = TRUE
            }).on("error",function(e){
                mysqlPool.release(mysql)
                process.log.error(e)
                response(ret)
            }).on("end",function(){
                mysqlPool.release(mysql)
                response(ret)
            })
        })
    },

    registerUser:function(user,response){
        mysqlPool.borrow(function(err,mysql){
            mysql.execute("insert into user (uid,phone,nick,password,sex,type,qq,renren) values (?,?,?,?,?)",[user.uid,
                user.phone,user.nick,user.password,user.sex,user.type,user.qq,user.renren]).on("error",function(){
                     mysqlPool.release(mysql)
                     response(FALSE)
                }).on("end",function(){
                    mysql.execute("insert into user_desc (uid,city,street,school,grade,money,point,desc)",[user.uid,
                        user.city,user.street,user.school,user.grade,user.money,user.point,user.desc]).on("error",function(){
                            mysqlPool.release(mysql)
                            response(FALSE)
                        }).on("end",function(){
                            mysqlPool.release(mysql)
                            response(TRUE)
                        })
                })


          /* mysql.execute("set autocommit = 0").on("end",function(){
               mysql.execute("insert into user (uid,phone,nick,password,sex,type,qq,renren) values (?,?,?,?,?)",[user.uid,
               user.phone,user.nick,user.password,user.sex,user.type,user.qq,user.renren]).on("error",function(){
                   mysql.execute("rollback").on("end",function(){
                       mysql.execute("set autocommit = 1").on("end",function(){
                           mysqlPool.release(mysql)
                           response(FALSE)
                       })
                   })
               }).on("end",function(){
                   mysql.execute("insert into user_desc (uid,city,street,school,grade,money,point,desc)",[user.uid,
                   user.city,user.street,user.school,user.grade,user.money,user.point,user.desc]).on("error",function(){
                           mysql.execute("rollback").on("end",function(){
                               mysql.execute("set autocommit = 1").on("end",function(){
                                   mysqlPool.release(mysql)
                                   response(FALSE)
                               })
                           })
               }).on("end",function(){
                           mysql.execute("commit").on("end",function(){
                               mysql.execute("set autocommit = 1").on("end",function(){
                                   mysqlPool.release(mysql)
                                   response(TRUE)
                               })
                           })
                   })
           }).on("error",function(){
                mysqlPool.release(mysql)
                response(FALSE)
           })
        })*/
    })},

    modifyPasswd:function(user,newpasswd,response){
        mysqlPool.borrow(function(err,mysql){
           var ret = FALSE;
           mysql.execute("update user set password=? where uid=? and password=?",[user.password,user.uid,newpasswd]).on("result",
           function(r){
               if(r.affected_rows==1){
                   ret = TRUE;
               }
           }).on("end",function(){
                   mysqlPool.release(mysql)
                   response(ret)
               })
        })
    },

    bindingQQ:function(qq,uid,response){
        mysqlPool.borrow(function(err,mysql){
            var ret = FALSE;
            mysql.execute("update user set qq=? where uid=?",[qq,uid]).on("result",
                function(r){
                    if(r.affected_rows==1){
                        ret = TRUE;
                    }
                }).on("end",function(){
                    mysqlPool.release(mysql)
                    response(ret)
                })
        })
    },

    bindingRenRen:function(renren,uid,response){
        mysqlPool.borrow(function(err,mysql){
            var ret = FALSE;
            mysql.execute("update user set renren=? where uid=?",[renren,uid]).on("result",
                function(r){
                    if(r.affected_rows==1){
                        ret = TRUE;
                    }
                }).on("end",function(){
                    mysqlPool.release(mysql)
                    response(ret)
                })
        })
    },

    login:function(user,token,expire,response){
        mysqlPool.borrow(function(err,mysql){
            var ret = false;
            var uid = "";
            mysql.execute("select uid from user where phone=? and password=?",[user.phone,user.password]).on("row",function(r){
                    ret = true;
                    uid = r.uid;
                }).on("end",function(){
                    mysqlPool.release(mysql)
                    if(ret){
                        var m = getMd5(user.id+"_"+new Date().getTime())
                        pool.borrow(function(err,redis){
                            redis.hmset("token_"+token,"uid",uid,"token",token,"expire",expire,function(err,reply){
                                redis.expireat("token_"+token,expire,function(){
                                    pool.release(redis)
                                });
                                response(m);
                            });
                        })
                    }else{
                        response("")
                    }
                })
        })
     },

    logout:function(token,respnose){
        pool.borrow(function(err,redis){
            redis.del("token_"+token,function(){
                pool.release(redis)
                response(TRUE)
            })
        })
    },

    sendCode:function(phone,expire,response){
        pool.borrow(function(err,redis){
            redis.set("code_"+phone,"123",function(err,reply){
                 redis.expireat("code_"+phone,expire,function(){
                    pool.release(redis)
                    response(TRUE)
                 })
            })
        })
    },

    verifyCode:function(phone,code,response){
         pool.borrow(function(err,redis){
            redis.get("code_"+phone,function(err,reply){
                 pool.release(redis);
                if(reply==code){
                    response(TRUE)
                }else{
                    response(FALSE)
                }
            })
         })
    },

    getCity:function(response){
        mysqlPool.borrow(function(err,mysql){
            var city= []
            mysql.execute("select city,cid from city").on("row",function(r){
                 city[r.city] = r.cid;
            }).on("error",function(){
                 mysqlPool.release(mysql)
                 response(city)
            }).on("end",function(){
                 mysqlPool.release(mysql)
                 response(city)
            })
        })
    },

    getStreet:function(city,response){
        mysqlPool.borrow(function(err,mysql){
            var street = []
            mysql.execute("select street,sid from street where cid=?",[city]).on("row",function(r){
                street[r.street] = r.sid
            }).on("error",function(){
                mysqlPool.release(mysql)
                response(street)
            }).on("end",function(){
                mysqlPool.release(mysql)
                response(street)
            })
        })
    },

    getSchool:function(street,response){
        mysqlPool.borrow(function(err,mysql){
            var school = []
            mysql.execute("select school,schoolid from school where sid=?",[street]).on("row",function(r){
                school[r.school] = r.schoolid
            }).on("error",function(){
                    mysqlPool.release(mysql)
                    response(school)
                }).on("end",function(){
                    mysqlPool.release(mysql)
                    response(school)
            })
        })
    },

    getGrade:function(school,response){
        mysqlPool.borrow(function(err,mysql){
            var grade = []
            mysql.execute("select grade,gid from grade where schoolid=?",[school]).on("row",function(r){
                grade[r.grade] = r.gid
            }).on("error",function(){
                    mysqlPool.release(mysql)
                    response(grade)
                }).on("end",function(){
                    mysqlPool.release(mysql)
                    response(grade)
                })
        })
    },

    getCounter:function(uid,response){
        pool.borrow(function(err,redis){
            redis.hgetall("counter_"+uid,function(err,reply){
                pool.release(redis)
                response(reply)
            })
        })
    },

    incCounter:function(uid,g,step,response){
        pool.borrow(function(err,redis){
            redis.hincby("counter_"+uid,g,step,function(err,reply){
                pool.release(redis)
                response(TRUE)
            })
        })
    },

    decrCounter:function(uid,g,step,response){
        pool.borrow(function(err,redis){
            redis.hincby("counter_"+uid,g,step,function(err,reply){
                pool.release(redis)
                response(TRUE)
            })
        })
    },

    getMd5:function(str){
        var hash = require('crypto').createHash('md5');
        return hash.update(str+"").digest('hex');
    }

})

