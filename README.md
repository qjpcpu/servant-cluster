基于etcd的资源分配器
===============================

![servant-cluster](https://github.com/qjpcpu/servant-cluster/raw/master/doc/servant-cluster.jpeg)

## 核心功能

* master主宰资源,一个资源被抽象成一张ticket，master负责将每个资源的使用权(ticket)分发给各个servant
* servant增加或者减少会触发资源重新分配,具体分配逻辑自定义

## 适用场景

* 资源相对固定,高度动态资源分发请使用消息队列

## 抽象术语

* master,整个系统唯一,实现上对应于master仅会存在于众多进程的众多协程的其中之一,当所在进程故障后会自动切换到剩余存活协程之一
* servant,从者协程,可以有无限个,决定于部署个数*MaxServantInProccess
* ticket,资源使用权,分为`SolidTicket`和`OnceTicket`两种,`SolidTicket`在同一进程内由某个servant使用完后可被其他协程再获取并使用,`OnceTicket`一旦分发,使用一次后即被丢弃
* fsn,主结构体,整个命名来源于`FatStayNight`,为了王之遗愿,加入圣杯战争

![fsn](https://github.com/qjpcpu/servant-cluster/raw/master/doc/fsn.jpg)

## example

参考示例代码[example.go](https://github.com/qjpcpu/servant-cluster/blob/master/example/example.go)

假定etcd地址是`127.0.0.1:2379`,执行示例代码:

```
# 可以同时执行多次命令,模拟多个servant进程
cd servant-cluster/example/ && go run example.go
```
