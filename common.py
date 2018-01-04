#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import time
import warnings

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": "http-dyn.abuyun.com",
    "port": "9020",
    "user": 'H2O9U25E301233VD',
    "pass": 'EA6E6072E8251749'
}

proxyMeta_https = "https://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": "http-dyn.abuyun.com",
    "port": "9020",
    "user": 'H2O9U25E301233VD',
    "pass": 'EA6E6072E8251749'
}
proxy_abuyun = {"http": proxyMeta, "https": proxyMeta_https}

# Kafka
KAFKA_ADDRESS = "192.168.6.54:9092"
KAFKA_ZOOKEEPER = ""
# KAFKA_ADDRESS = "192.168.3.1:9092,192.168.3.2:9092,192.168.3.3:9092"
# KAFKA_ZOOKEEPER = "192.168.3.3:2181,192.168.3.2:2181,192.168.3.1:2181/kafka"

KAFKA_CONSUMER_COOKIE = "auth.taobao.v4"
KAFKA_CONSUMER_COOKIE_GROUP_ID = "taobaospider"
KAFKA_PRODUCER_TB = "auth.taobao.notify.v4"
KAFKA_PRODUCER_LOAN = "crawl.result.taobao.loan.v4"

# Redis
TRANSFER_STATION_NAME = "tb_distributer"
REDIS_HOST = "192.168.6.213"
REDIS_PORT = 6379
REDIS_DB_INDEX = 229
REDIS_PWD = 'redis213daianla2016'
# REDIS_HOST = "r-bp1af1df9cb542a4.redis.rds.aliyuncs.com"
# REDIS_PORT = 6379
# REDIS_DB_INDEX = 99
# REDIS_PWD = 'D3hkip2j1qJ'

# MongoDB
MONGODB_HOST = '192.168.6.54'
MONGODB_PORT = 27017
MONGODB_DATABASE = 'taobaospider'
MONGODB_USERNAME = 'test'
MONGODB_PASSWORD = 'test123'
# MONGODB_HOST = 's-bp11abf0bb3f5ed4.mongodb.rds.aliyuncs.com'
# MONGODB_PORT = 3717
# MONGODB_DATABASE = 'taobaospider'
# MONGODB_USERNAME = 'taobaospider'
# MONGODB_PASSWORD = '1as4325gvf98jhf2'
MONGODB_COLL_ALIPAY_WEALTH = 'RawTbAccountAlipayWealth'
MONGODB_COLL_DELIVER_ADDRESS = 'RawTbAccountDeliverAddress'
MONGODB_COLL_TRADE_INFO = 'RawTbAccountTradeInfo'
MONGODB_COLL_USER_INFO = 'RawTbAccountUserInfo'


def getProxy():
    return proxy_abuyun
    # return None


def getAlipayHeader(cookies):
    return {
        "Host": "i.taobao.com",
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://i.taobao.com/my_taobao.htm?spm=a21bo.50862.1997525045.1.Uc87Cm",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,pl;q=0.2",
        "Cookie": cookies
    }


def getOrderHeader(cookies):
    return {
        "Host": "unsz.api.m.taobao.com",
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://h5.m.taobao.com/mlapp/olist.html?spm=a2141.7756461.2.6",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,pl;q=0.2",
        "Cookie": cookies
    }


def getOrderHeader_PC(cookies):
    return {
        "Host": "buyertrade.taobao.com",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,pl;q=0.2",
        "Cookie": cookies
    }


def getOrderHeader_PC_async(cookies):
    return {
        "Host": "buyertrade.taobao.com",
        "Connection": "keep-alive",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://buyertrade.taobao.com",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://buyertrade.taobao.com/trade/itemlist/list_bought_items.htm?pageNum=3&pageSize=15&prePageNo=2"
    }


def getDetailHeader(cookies, host):
    return {
        "Host": host,
        "Referer": "https://h5.m.taobao.com/mlapp/odetail.html?bizOrderId=3187169481617278&archive=false&spm=a2141.7631731.0.i1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,pl;q=0.2",
        "Cookie": cookies
    }


def getBaseHeader(cookies):
    return {
        "Host": "member1.taobao.com",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,pl;q=0.2",
        "Cookie": cookies
    }


# 生成13位时间戳
def getDate13():
    return '%d' % (time.time() * 1000)


def printWarning(info):
    warnings.warn_explicit(info, FutureWarning, '', 0)
