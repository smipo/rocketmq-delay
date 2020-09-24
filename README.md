添加RocketMQ延时队列,支持任意延时,RocketMQ版本4.7.1
****
一.安装  
新添加了jar,分别是:jackson-annotations-2.9.9.jar jackson-databind-2.9.9.jar jackson-core-2.9.9.jar joda-time-2.9.9.jar
下载RocketMQ4.7.1二进制包,将新加的jar包添加到lib目录下,在将本项目的store项目maven打包替换掉原有的store jar.按原有启动方式启动。   

二.配置  
在rocketmq broker配置基础上添加延时队列文件根路径delayPathDir,不配置此参数,默认工作空间/data文件下存储,如:  
delayPathDir=C:\\rocketmq\\delay
在delayPathDir文件目录下需存在delay.properties文件,若不配置或者delay.properties文件不存在,则全为默认参数,delay.properties参数如：  
store.root=文件存储目录,默认工作空间/data目录下  
log.expired.delete.enable=是否删除存储文件,true 删除 false不删除 默认false  
log.retention.check.interval.seconds=多少秒执行一次删除操作,默认60秒  
dispatch.log.keep.hour=保留多少个小时存储的文件,默认72个小时  
  
三.延时代码  
可参考example中的simple中的DelayProducer类：  
msg.setDelayTimeLevel(1);表示是延时消息,此参数必须大于0  
msg.putUserProperty("scheduleTime","");此参数存储在map属性中,表示延后多久执行,必须是计算好的毫秒时间,是long整形,比如1600850679423,
若此参数小于服务器当前时间,则会立即执行。  
以上两个必传参数,若不传scheduleTime属性则按照rocketmq原有的逻辑执行,若传scheduleTime属性则rocketmq原有的逻辑不会执行。 

****
[Apache RocketMQ](https://rocketmq.apache.org) 的地址



