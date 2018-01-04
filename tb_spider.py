#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# 基于淘宝登录cookies,抓取数据
import uuid
import random
import json
import time
import bs4
import traceback
import urlparse
import requests
from requests.exceptions import ConnectionError
import urllib3.contrib.pyopenssl
import common
from pykafka import KafkaClient
import redis
import pymongo
from pymongo.errors import DuplicateKeyError
from hgjAndLoan import tbLoan, sycm
import logging
from logging.handlers import TimedRotatingFileHandler

r_client = redis.StrictRedis(
    host=common.REDIS_HOST,
    port=common.REDIS_PORT,
    db=common.REDIS_DB_INDEX,
    password=common.REDIS_PWD
)

mongo_client = pymongo.MongoClient(host=common.MONGODB_HOST, port=common.MONGODB_PORT)
mongo_db = mongo_client[common.MONGODB_DATABASE]
mongo_db.authenticate(name=common.MONGODB_USERNAME, password=common.MONGODB_PASSWORD)
coll_user_info = mongo_db[common.MONGODB_COLL_USER_INFO]
coll_deliver_address = mongo_db[common.MONGODB_COLL_DELIVER_ADDRESS]
coll_trade_info = mongo_db[common.MONGODB_COLL_TRADE_INFO]

urllib3.disable_warnings()
urllib3.contrib.pyopenssl.inject_into_urllib3()
requests.packages.urllib3.disable_warnings()

log_path = 'log_spider.log'
log = logging.getLogger()
log.setLevel(logging.DEBUG)
log_handler = TimedRotatingFileHandler(
    filename=log_path,
    when='MIDNIGHT',
    interval=1,
    backupCount=3
)
formatter = logging.Formatter('%(asctime)s | %(filename)s, [line:%(lineno)d] | %(levelname)s : %(message)s')
log_handler.setFormatter(formatter)
log.addHandler(log_handler)

totalNumber = 0  # 订单数总计
userinfo_obj = {}  # 基础信息(会员名/邮箱/手机/真实姓名/支付宝账号/身份证号)
loan_data = {}
alipaywealth_obj = {}  # 支付宝额度/花呗额度
deliveraddress_arr = []  # 收货地址
tradeinfo_arr = []  # 订单列表及详情
sellerinfo_arr = []  # 卖家信息(地区/电话)
tb_cookies = ''  # 用于淘宝订单cookie
wsd_cookies = ''  # 用于网商贷cookie
sycm_cookies = ''  # 用于好管家cookie
task_id = ''  # 任务id(作为mongdb的_id)
userId = ''  # 由原始cookies参数获得
nickName = ''  # 用户nickname
serial_number = ''  # 订单流水号


def edit_log(content):
    log.info(content)


def record_exception(content):
    key = 'tbSpider_{}_{}'.format(task_id, nickName)
    r_client.lpush(key, '{}_{}'.format(time.time(), content))
    r_client.expire(key, 24 * 60 * 60)


def get_date():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# 数据入库mongodb
def insert_mongo(coll, data):
    if isinstance(data, dict):
        try:
            coll.insert_one(data)
            pass
        except DuplicateKeyError:
            pass


def statistic_individual_succ_percentage(total_p):
    global task_id
    global nickName
    global tradeinfo_arr
    global totalNumber
    # TODO: succ_judge is true when get 3 pages that total pages more than 6 at the front and back of detail otherwise is false
    succ_judge = False
    tradeinfo_arr_len = len(tradeinfo_arr)
    if total_p > 6:
        if tradeinfo_arr_len >= 75:
            succ_judge = True
    else:
        succ_judge = True

    content = "statistic_individual_succ_percentage: task_id={task_id},nickName={nickName}," \
              "total_p={total_p},totalNumber={totalNumber},get_pages={tradeinfo_arr_len},succ_judge={succ_judge}".format(
        task_id=task_id,
        nickName=nickName,
        total_p=total_p,
        totalNumber=totalNumber,
        tradeinfo_arr_len=tradeinfo_arr_len,
        succ_judge=succ_judge
    )
    edit_log(content)


# 获取一个订单详情
def getOneDetail_FromPC(tradeObj):
    if tradeObj is None:
        edit_log('getOneDetail_FromPC: tradeObj is None.')
        return False
    global tb_cookies
    istamll = tradeObj['istmall']
    oid = tradeObj['trade_id']
    if istamll:
        detailUrl = 'https://trade.tmall.com/detail/orderDetail.htm?spm=a1z09.2.0.0.BozM3g&bizOrderId=' + oid
    else:
        detailUrl = 'https://tradearchive.taobao.com/trade/detail/trade_item_detail.htm?spm=a1z09.2.0.0.QYb5At&bizOrderId=' + oid

    # if oid == '2664067492797278':
    #     tmp = 0

    for i in range(3):  # 每个订单尝试3遍
        try:
            with requests.Session() as s:
                s.keep_alive = False
                r = s.get(detailUrl, headers=common.getDetailHeader(tb_cookies, urlparse.urlparse(detailUrl).netloc),
                          allow_redirects=False, verify=False, proxies=common.getProxy(), timeout=20)
                if r.status_code is not 200:
                    if r.status_code == 302:
                        jumpUrl = r.headers['Location']
                        if 'err.taobao.com' in jumpUrl:
                            edit_log('getOneDetail_FromPC: 302 err.taobao.com')
                        elif 'login.taobao.com' in jumpUrl:
                            edit_log('getOneDetail_FromPC: 302 login.taobao.com')
                        else:
                            istamll = jumpUrl.__contains__('tmall.com')
                            detailUrl = jumpUrl
                            tradeObj['istmall'] = istamll
                    continue
        except Exception, e:
            edit_log("getOneDetail_FromPC error " + str(traceback.print_exc()))
            continue

        if r.text == '':
            edit_log('getOneDetail_FromPC: r.text is empty')
            continue

        # if oid == '3110118206506620':
        #     tmp = 0

        # 解析html,获取卖家信息
        try:
            base_Node = bs4.BeautifulSoup(r.text, 'lxml')
            script_Node = base_Node.find_all("script")
            title = base_Node.find("title").get_text()
            if u'天猫' in title or r.text.__contains__('var detailData'):
                # 1.天猫类详情
                err_Node = base_Node.find("div", attrs={'class': 'error-container'})
                if err_Node is not None:
                    edit_log(u'很抱歉，系统繁忙，请稍后再试')
                elif handleDetail_tm(tradeObj, script_Node):
                    return True
                continue
            elif u'交易详情' in title:
                # 2.淘宝类详情(https://tradearchive.taobao.com/trade/detail/trade_item_detail.htm?spm=a1z09.2.0.0.QYb5At&bizOrderId=2741165884137278)
                # 注意: 淘宝又分2类,页面直接返回数据和页面json返回,json暂未遇到
                if handleDetail_tb(tradeObj, r, base_Node):
                    return True
                continue
            elif u'出错了' in title:
                err_Node = base_Node.find("div", attrs={'class': 'error-notice-hd'})
                if err_Node is not None:
                    edit_log(u'getOneDetail_FromPC: 出错了 ' + err_Node.get_text().strip())
                    # ToDo: 抓取wap版详情
                else:
                    edit_log(u'getOneDetail_FromPC: 出错了. r.text: ' + str(r.text))
                break
            else:
                # ToDo
                edit_log(u'getOneDetail_FromPC: 新订单详情页面未识别 r.text: ' + str(r.text))
                break
        except Exception, e:
            edit_log("traceback: " + str(traceback.print_exc()))
            continue
    return False


def getDetails_FromPC(arr, bAdd):
    if arr is None or len(arr) == 0:
        edit_log('getDetails_FromPC: arr is empty.')
        return
    global tradeinfo_arr
    detailCount = 0
    failCount = 0
    count = 1
    for tradeObj in arr:
        count += 1
        if getOneDetail_FromPC(tradeObj):
            detailCount += 1
            if bAdd:  # 是否加入全局arr
                tradeinfo_arr.append(tradeObj)
        else:
            failCount += 1


# 获取订单列表
def getOrderList_FromPC():
    try:
        tradeinfo_arr_TMP = []  # 临时订单列表集合,用于获取多页订单列表后,统一获取订单详情
        # 第一页
        b, tradeArr, totalPage = getOnePageList_FromPC(1)
        total_p = totalPage
        if b and len(tradeArr) > 0:
            [tradeinfo_arr_TMP.append(tradeObj) for tradeObj in tradeArr]
        else:
            edit_log('getOrderList_FromPC: get first page fail.')
            return

        # 计算要获取的页数(前后各3页)
        pages = []
        remain_page = []
        if totalPage < 6:
            [pages.append(p) for p in range(2, totalPage + 1)]
        else:
            pages = [totalPage, 2, 3, totalPage - 2, totalPage - 1]
            [remain_page.append(p1) for p1 in xrange(4, totalPage - 2)]

        # [pages.append(p) for p in range(2, totalPage + 1)]

        # TODO: get 3 pages at the front and back of buyer detail
        for pIdx in pages:
            b, tradeArr, totalPage = getOnePageList_FromPC(pIdx)
            if b and len(tradeArr) > 0:
                [tradeinfo_arr_TMP.append(tradeObj) for tradeObj in tradeArr]
        # TODO: get 3 pages at the front and back of seller detail
        getDetails_FromPC(tradeinfo_arr_TMP, True)
        tradeinfo_arr_TMP = []

        # TODO: obtain remaining detail of buyer and seller
        for pIdx in remain_page:
            b, tradeArr, totalPage = getOnePageList_FromPC(pIdx)
            if b and len(tradeArr) > 0:
                [tradeinfo_arr_TMP.append(tradeObj) for tradeObj in tradeArr]
        getDetails_FromPC(tradeinfo_arr_TMP, True)

        # TODO: statistic individual succ percentage
        # statistic_individual_succ_percentage(total_p)
    except Exception, e:
        edit_log('getOrderList_FromPC: Exception' + " traceback: " + str(traceback.print_exc()))


# 解析天猫类型订单详情页html
def handleDetail_tm(tradeObj, script_Node):
    try:
        # 1.天猫类详情
        # https://trade.tmall.com/detail/orderDetail.htm?spm=a1z09.2.0.0.BozM3g&bizOrderId=6258248620207278
        if script_Node is None or len(script_Node) == 0:
            edit_log('handleDetail_tm: script not found')
            return False

        jdata = None
        for s in script_Node:
            jsStr = str(s)
            idx = jsStr.find('detailData')
            if idx is -1:
                continue
            jdata = json.loads(jsStr[jsStr.find('{'):jsStr.rfind('}') + 1])
            break

        if jdata is None:
            edit_log('handleDetail_tm: json not found(var detailData=)')
            return False

        county = ''
        city = ''
        province = ''
        totalAmount = ''
        deliver_addr = ''
        deliver_name = ''
        deliver_mobilephone = ''
        deliver_postcode = ''
        fapiao = ''
        seller_name = ''
        seller_realName = ''
        seller_city = ''
        seller_tel = ''
        global sellerinfo_arr
        # 金额信息
        if jdata.has_key('amount') is False \
                or jdata['amount'].has_key('count') is False \
                or len(jdata['amount']['count']) == 0:
            edit_log('handleDetail_tm: json do not cantains amount')
            return False

        for count in jdata['amount']['count'][0]:
            for content in count['content']:
                if content.has_key('data') \
                        and content['data'].has_key('money') \
                        and content['data']['titleLink']['text'] == '订单总价':
                    totalAmount = content['data']['money']['text']
                    break

        # 基本信息
        if jdata.has_key('basic') is False \
                or jdata['basic'].has_key('lists') is False \
                or len(jdata['basic']['lists']) == 0:
            # edit_log('handleDetail_tm: json do not cantains basic')
            return False

        # 遍历订单
        for bssic in jdata['basic']['lists']:
            key = bssic['key']
            for content in bssic['content']:
                val = content['text'].strip()
                if len(val) == 0:
                    continue

                if key == '收货地址':
                    if len(val) == 0 \
                            or '不需要收货人' in val \
                            or '不需要收货地址' in val \
                            or '不需收货地址' in val \
                            or '不需收货地址' in val:
                        continue
                    # "冯仁彬,86-13608011909,四川省 成都市 武侯区 石羊场街道 府城大道西段399号天府新谷6栋706 ,610041"
                    arrTmp = val.split(',')
                    valLen = len(arrTmp)
                    try:
                        if valLen == 4:
                            deliver_name = arrTmp[0]
                            deliver_mobilephone = arrTmp[1]
                            deliver_addr = arrTmp[2]
                            deliver_postcode = arrTmp[3]

                            arrTmp = deliver_addr.split(' ')
                            province = arrTmp[0]
                            city = arrTmp[1]
                            county = arrTmp[2]
                        elif valLen == 3:
                            deliver_name = arrTmp[0]
                            deliver_mobilephone = arrTmp[1].strip()
                            deliver_postcode = arrTmp[2]
                        else:
                            # edit_log('handleDetail_tm: 未识别的收货地址格式 val: ' + str(val))
                            pass
                    except IndexError:
                        edit_log('handleDetail_tm: 未识别的收货地址格式 val: ' + str(val))
                        pass

                elif key == u'发票抬头':  # 判断是否公司购买
                    fapiao = val
                elif key == u'商家':
                    if content['type'] == 'html':
                        shopNode = bs4.BeautifulSoup(val, 'lxml')
                        if shopNode is not None:
                            seller_name = shopNode.get_text()
                    else:
                        for t in content['moreList']:
                            if len(t['content']) == 0 or t['content'][0].has_key('text') is False:
                                continue
                            val = t['content'][0]['text']
                            if t['key'] == u'真实姓名':
                                seller_realName = val
                            elif t['key'] == u'城市':
                                seller_city = val
                            elif t['key'] == u'联系电话':
                                seller_tel = val

        # 宝贝信息(一个订单可能包含多个宝贝)
        if jdata.has_key('orders') is False or jdata['orders'].has_key('list') is False or len(
                jdata['orders']['list']) == 0:
            # edit_log('handleDetail_tm: json do not cantains orders')
            return False

        productObj = []
        for order in jdata['orders']['list'][0]['status']:
            productTitle = order['subOrders'][0]['itemInfo']['title']
            if u'增值服务' in productTitle or u'保险服务' in productTitle:
                continue

            # productImgUrl = order['subOrders'][0]['itemInfo']['pic']
            productPrice = order['subOrders'][0]['priceInfo'][0]['text']
            order_status = order['statusInfo'][0]['text']
            quantity = order['subOrders'][0]['quantity']
            productObj.append({'item_name': productTitle.encode('utf8'), 'item_price': productPrice,
                               'order_status': order_status.encode('utf8'), 'quantity': quantity})

        # 时间(拍下商品|付款到支付宝|卖家发货|确认收货|评价)
        trade_createtime = ''
        if jdata.has_key('stepbar') is False \
                or jdata['stepbar'].has_key('options') is False \
                or len(jdata['stepbar']['options']) == 0:
            # edit_log('handleDetail_tm: json do not cantains stepbar[options]')
            pass
        else:
            # 拍下商品时间
            for t in jdata['stepbar']['options']:
                if t.has_key('time') is False or t['content'] != u'拍下商品':
                    continue
                trade_createtime = t['time']
                tradeObj['trade_createtime'] = trade_createtime
                break

        seller_info = {
            'name': seller_name,  # seller_name
            'area': seller_city,
            'phone': seller_tel,
            'nick': seller_name,
            'istamll': True
        }
        sellerinfo_arr.append(seller_info)
        tradeObj['seller_info'] = seller_info
        tradeObj['deliverinfo'] = {
            'fapiao': fapiao,
            'name': deliver_name,
            'phone_no': deliver_mobilephone,
            'full_address': deliver_addr,
            'zip_code': deliver_postcode,
            'city': city,
            'province': province,
            'county': county
        }
        return True
    except Exception, e:
        edit_log("handleDetail_tm error " + str(traceback.print_exc()))


# 解析淘宝类型订单详情页html
def handleDetail_tb(tradeObj, r, base_Node):
    try:
        if r.text.find('var data') == -1:
            # 页面返回数据,直接解析html
            nike_Node = base_Node.find("span", attrs={'class': 'nickname'})
            if nike_Node is not None:
                seller_nick = nike_Node.get_text()
            else:
                seller_nick = ''

            name_Node = base_Node.find("span", attrs={'class': 'name'})
            if name_Node is not None:
                seller_name = name_Node.get_text()
            else:
                seller_name = ''

            city_Node = base_Node.find("span", attrs={'class': 'city'})
            if city_Node is not None:
                seller_city = city_Node.get_text()
            else:
                seller_city = ''

            tel_Node = base_Node.find("span", attrs={'class': 'tel'})
            if tel_Node is not None:
                seller_tel = tel_Node.get_text()
            else:
                seller_tel = ''

            price_Node = base_Node.select("div.get-money strong")
            if price_Node is None or len(price_Node) == 0:
                price = ''
            else:
                price = price_Node[0].string

            trade_createtime_Node = base_Node.find("span", attrs={'class': 'trade-time'})
            trade_createtime = trade_createtime_Node.get_text() if trade_createtime_Node is not None else ''
            tradeObj['trade_createtime'] = trade_createtime

            productObj = []
            productTr_Node = base_Node.select("tr.order-item")
            if productTr_Node is None or len(productTr_Node) == 0:
                productObj = []
            else:
                for tr in productTr_Node:
                    productTitle_Node = tr.find("td", attrs={'class': 'item'})
                    productTitle = '' if productTitle_Node is None else productTitle_Node.get_text().strip()

                    # 633.90
                    # 满439减20元:省20.00元
                    productPrice_Node = tr.find("td", attrs={'class': 'order-price'})
                    productPrice = '' if productPrice_Node is None else productPrice_Node.contents[0].strip()
                    status_Node = tr.find("td", attrs={'class': 'status'})
                    order_status = '' if status_Node is None else status_Node.get_text().strip()
                    quantity_Node = tr.find("td", attrs={'class': 'num'})
                    quantity = '' if quantity_Node is None else quantity_Node.get_text().strip()

                    productObj.append({'productTitle': productTitle.encode('utf8'), 'productPrice': productPrice,
                                       'order_status': order_status.encode('utf8'), 'quantity': quantity})

            county = ''
            city = ''
            province = ''
            fapiao = ''
            deliver_addr = ''
            deliver_name = ''
            deliver_mobilephone = ''
            deliver_postcode = ''
            global sellerinfo_arr
            info_Nodes = base_Node.find_all("table", attrs={'class': 'simple-list logistics-list'})
            if info_Nodes is not None and len(info_Nodes) != 0:
                for info_Node in info_Nodes:
                    td_Nodes = info_Node.find_all('td')
                    for tdIdx in range(len(td_Nodes)):
                        td = td_Nodes[tdIdx]
                        if '收货地址' in td.get_text():  # '收货地址的下一个td即地址信息
                            deliver_addr = td_Nodes[tdIdx + 1].get_text().strip()
                            if len(deliver_addr) == 0 \
                                    or '不需要收货人' in deliver_addr \
                                    or '不需要收货地址' in deliver_addr \
                                    or '不需收货人' in deliver_addr \
                                    or '不需收货地址' in deliver_addr:
                                deliver_addr = '不需要收货人,不需要收货地址'
                                break

                            addrArr = deliver_addr.split('，')
                            if len(addrArr) == 5:
                                deliver_name = addrArr[0].strip()
                                deliver_mobilephone = addrArr[1].strip()
                                deliver_addr = addrArr[3].strip()
                                deliver_postcode = addrArr[4].strip()

                                addrArr = deliver_addr.split(' ')
                                province = addrArr[0]
                                city = addrArr[1] if len(addrArr) >= 2 else ''
                                county = addrArr[2] if len(addrArr) >= 3 else ''
                            else:
                                addrArr = filter(lambda x: x, deliver_addr.split(' '))
                                if len(addrArr) == 3:
                                    deliver_name = addrArr[0].strip()
                                    deliver_mobilephone = addrArr[1].rstrip(',').strip()
                                    deliver_postcode = addrArr[2].strip()
                                    deliver_addr = ''
                                elif len(addrArr) == 5:
                                    name_phone_postcode = addrArr[0].replace("\r", "").replace("\t", "").replace("\n",
                                                                                                                 "")
                                    name_phone_postcode = name_phone_postcode.split('，')
                                    deliver_name = name_phone_postcode[0].strip()
                                    deliver_mobilephone = name_phone_postcode[1].strip()
                                    deliver_postcode = addrArr[4].strip().replace('，', '')
                                    deliver_addr1 = addrArr[3].strip()

                                    addrArr1 = deliver_addr1.split('，')
                                    province = addrArr1[0]
                                    city = addrArr[1].strip()
                                    county = ""  # addrArr1[2]
                                else:
                                    tmp = 0

                            break
                        elif '发票' in td.get_text():
                            fapiao = td_Nodes[tdIdx + 1].get_text().strip()

            seller_info = {
                'name': seller_name,  # seller_name
                'area': seller_city,
                'phone': seller_tel,
                'nick': seller_nick,
                'istamll': False
            }
            sellerinfo_arr.append(seller_info)
            tradeObj['seller_info'] = seller_info
            tradeObj['deliverinfo'] = {
                'fapiao': fapiao,
                'name': deliver_name,
                'phone_no': deliver_mobilephone,
                'full_address': deliver_addr,
                'zip_code': deliver_postcode,
                'city': city,
                'province': province,
                'county': county
            }


        else:
            # 页面内嵌json形式返回数据(暂未遇到)
            script_Node = base_Node.find_all("script")
            if script_Node is None:
                # edit_log('handleDetail_tb: script not found')
                return False

            jdata = None
            for s in script_Node:
                jsStr = str(s)
                idx = jsStr.find('var data')
                if idx is -1:
                    continue
                jdata = json.loads(jsStr[jsStr.find('{'):jsStr.rfind('}') + 1])
                break

            if jdata is None:
                # edit_log('handleDetail_tb: json not found(var data=)')
                return False

            # 判断json是否满足预期
            if jdata.has_key('deliveryInfo') is False \
                    or jdata.has_key('mainOrder') is False \
                    or jdata.has_key('orderBar') is False:
                # edit_log('handleDetail_tb: json do not cantains deliveryInfo/mainOrder/orderBar')
                return False

            deliver_addr = jdata['deliveryInfo']['address']
            seller_nick = jdata['mainOrder']['seller']['nick']
            seller_city = jdata['mainOrder']['seller']['city']
            seller_realName = jdata['mainOrder']['seller']['name']
            seller_tel = jdata['mainOrder']['seller']['phoneNum']
            order_status = jdata['mainOrder']['statusInfo']['text']

            trade_createtime = ''
            for n in jdata['orderBar']['nodes']:
                if n['text'] == '拍下商品':
                    trade_createtime = n['date']
                    break

            price = jdata['mainOrder']['payInfo']['actualFee']['value']
            productObj = []
            for o in jdata['mainOrder']['subOrders']:
                productPrice = o['priceInfo']
                quantity = o['quantity']
                productTitle = o['itemInfo']['title']
                order_status = o['tradeStatus'][0]['content'][0]['value']
                productObj.append({'productTitle': productTitle.encode('utf8'), 'productPrice': productPrice,
                                   'order_status': order_status.encode('utf8'), 'quantity': quantity})
        return True
    except Exception, e:
        edit_log("handleDetail_tb error " + str(traceback.print_exc()))


# 获取一页订单
def getOnePageList_FromPC(pageIdx):
    data = {
        "buyerNick": "",
        "dateBegin": "0",
        "dateEnd": "0",
        # "lastStartRow": "2483207872_9223370583584484807_1458370011897278_1458370011897278", # 待修改?
        "lastStartRow": "",  # 待修改?
        "logisticsService": "",
        "options": "0",
        "orderStatus": "",
        "pageNum": str(pageIdx),
        "pageSize": "15",
        "queryBizType": "",
        "queryOrder": "desc",
        "rateStatus": "",
        "refund": "",
        "sellerNick": "",
        "prePageNo": str(pageIdx - 1) if pageIdx > 1 else '2'
    }
    global totalNumber
    global tb_cookies
    jdata = None
    with requests.Session() as s:
        s.keep_alive = False
        for _ in range(3):  # 获取一页PC订单列表,尝试3次
            try:
                url = 'https://buyertrade.taobao.com/trade/itemlist/asyncBought.htm?action=itemlist/BoughtQueryAction&event_submit_do_query=1&_input_charset=utf8'
                cookies = dict()
                for item in tb_cookies.split(';'):
                    key, value = item.strip().split('=', 1)
                    cookies[key] = value
                r = s.post(url, data=data, headers=common.getOrderHeader_PC_async(tb_cookies), verify=False,
                           allow_redirects=False, timeout=20, cookies=cookies, proxies=common.getProxy())

                if r.status_code is not 200:
                    edit_log('getOnePageList_FromPC: ' + str(r.status_code))
                    # todo
                    record_exception('getOnePageList_FromPC: status error ' + str(r.status_code))
                    continue
                if 'sec.taobao.com' in r.text or 'login.taobao.com' in r.text:
                    # jerr = json.loads(r.text)
                    # secUrl = jerr['url']
                    # b, newUrl = handleAnti(secUrl)
                    edit_log('getOnePageList_FromPC: antiSpider !')
                    # todo
                    record_exception('getOnePageList_FromPC: antiSpider !')
                    continue
                jdata = json.loads(r.text)
                break
            except ConnectionError:
                record_exception('getOnePageList_FromPC: requests ConnectionError')
            except Exception, e:
                record_exception('getOnePageList_FromPC: {}'.format(e))
                jdata = None

    try:
        if jdata is None:
            edit_log('getOnePageList_FromPC: json not found')
            return False, None, None
        if jdata.has_key('error') and jdata['error'] != '':
            edit_log('getOnePageList_FromPC: err ' + jdata['error'])
            return False, None, None
        if jdata.has_key('mainOrders') is False or jdata.has_key('page') is False:
            edit_log('getOnePageList_FromPC: mainOrders/page is empty')
            return False, None, None

        totalNumber = int(jdata['page']['totalNumber'])  # 总订单数
        currentPage = int(jdata['page']['currentPage'])  # 当前页
        totalPage = int(jdata['page']['totalPage'])  # 总页数  # ToDo:totalPage==0,无购买记录的情况
    except Exception, e:
        edit_log("getOnePageList_FromPC error " + str(traceback.print_exc()))
        # todo
        record_exception("getOnePageList_FromPC: {}".format(e))
        return False, None, None

    tradeArr = []
    for o in jdata["mainOrders"]:  # 遍历每页15条订单
        actual_fee = 0
        post_type = ''
        trade_status = trade_createtime = seller_id = seller_nick = seller_shopname = trade_text = original = ''

        try:
            isB2C = o['orderInfo']['b2C'] if o['orderInfo'].has_key('b2C') else False
            trade_id = o['id']
            trade_status = o['extra']['tradeStatus']
            trade_createtime = o['orderInfo']['createTime']
            actual_fee = float(o['payInfo']['actualFee'])
            post_type = o['payInfo']['postType'] if o['payInfo'].has_key('postType') else ''
            if '_CLOSED' not in trade_status and "TRADE_FINISHED" != trade_status:
                continue
            # if '_CLOSED' in trade_status:
            #     actual_fee = 0
            if not actual_fee:
                continue
            # if '24028000364835113' in trade_id:
            #     tmp = 0

            if o.has_key('seller'):
                seller_id = o['seller']['id']
                seller_nick = o['seller']['nick']
                seller_shopname = o['seller']['shopName'] if o['seller'].has_key('shopName') else ''
            trade_text = o['statusInfo']['text']
            subOrders = o['subOrders']
            sub_orders = []
            if len(subOrders) == 0:
                pass
            else:
                for odr in subOrders:
                    item_name = odr['itemInfo']['title']
                    if item_name.__contains__(u'增值服务') or item_name.__contains__(u'保险服务'):
                        continue

                    if len(subOrders) == 1 and '_CLOSED' in trade_status:
                        real_total = 0
                    else:
                        real_total = odr['priceInfo']['realTotal']
                        if odr['priceInfo'].has_key('original'):
                            original = odr['priceInfo']['original']
                        else:
                            original = 0

                        if odr.has_key('operations'):
                            operations = odr['operations']
                            if len(operations) != 0:
                                for op in operations:
                                    if op.has_key('text') and op['text'] == u'查看退款':
                                        real_total = 0
                                        break

                    sub_orders.append({
                        'item_name': item_name,
                        'item_id': odr['itemInfo']['id'] if odr['itemInfo'].has_key('id') else '',
                        'item_url': odr['itemInfo']['itemUrl'] if odr['itemInfo'].has_key('itemUrl') else '',
                        'item_pic': odr['itemInfo']['pic'] if odr['itemInfo'].has_key('pic') else '',
                        'original': original,
                        'real_total': real_total,
                        'quantity': odr['quantity'],
                        'trade_id': odr['id'] if odr.has_key('id') else ''
                    })

            tradeObj = {
                "trade_id": trade_id,
                "trade_status": trade_status,
                "trade_createtime": trade_createtime,
                "actual_fee": actual_fee,
                "post_type": post_type,
                "seller_id": seller_id,
                "seller_nick": seller_nick,
                "seller_shopname": seller_shopname,
                "trade_text": trade_text,
                "istmall": isB2C,
                'sub_orders': sub_orders
            }

            # getOneDetail_FromPC(tradeObj, True)    # 立即获取该订单详情
            tradeArr.append(tradeObj)
        except Exception, e:
            edit_log("getOrderList_FromPC error: {}".format(e))
            # todo
            record_exception("getOrderList_FromPC: {}".format(e))
    return True, tradeArr, totalPage


# 获取支付宝额度/花呗额度
def getAlipayHuabei():
    try:
        balance = 0  # 账户余额
        total_profit = 0  # 余额宝历史累计收益
        total_quotient = 0  # 余额宝
        huabei_creditamount = 0  # 花呗 可用额度
        huabei_totalcreditamount = 0  # 花呗 消费额度
        global alipaywealth_obj
        global tb_cookies
        global task_id
        url_alipay = 'https://i.taobao.com/my_taobao_api/alipay_blance.json?_ksTS=' + common.getDate13() + '_' + str(
            random.randint(111, 999))
        url_huabei = 'https://i.taobao.com/my_taobao_api/getHuaBeiBalance.json?_ksTS=' + common.getDate13() + '_' + str(
            random.randint(111, 999))

        for i in range(3):  # 支付宝尝试3遍
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(url_alipay, headers=common.getAlipayHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:

                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                pass
                        continue

                    # {"data":{"balance":"499.50","sign":true,"totalProfit":"1026.62","totalQuotient":"500"}}
                    jdata = json.loads(r.text)
                    balance = float(jdata['data']['balance']) if jdata['data']['balance'] != '' else 0
                    total_profit = float(jdata['data']['totalProfit']) if jdata['data'][
                                                                              'totalProfit'] != '' else 0
                    total_quotient = float(jdata['data']['totalQuotient']) if jdata['data'][
                                                                                  'totalQuotient'] != '' else 0
                    break
            except Exception, e:
                continue

        for i in range(3):  # 花呗尝试3遍
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(url_huabei, headers=common.getAlipayHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:

                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                pass
                        continue

                    # {"data":{"huaBeiCreditAmount":"0","huaBeiCreditAuthResult":"F","huaBeiTotalCreditAmount":"0"}}
                    jdata = json.loads(r.text)
                    huabei_creditamount_str = jdata['data']['huaBeiCreditAmount']
                    huabei_totalcreditamount_str = jdata['data']['huaBeiTotalCreditAmount']

                    huabei_creditamount = float(
                        float(huabei_creditamount_str) / 100 if huabei_creditamount_str != '0' else 0)
                    huabei_totalcreditamount = float(
                        float(huabei_totalcreditamount_str) / 100 if huabei_totalcreditamount_str != '0' else 0)
                    break
            except Exception, e:
                continue
        alipaywealth_obj = {'_id': task_id, 'balance': balance, 'total_profit': total_profit,
                            'total_quotient': total_quotient, 'huabei_creditamount': huabei_creditamount,
                            'huabei_totalcreditamount': huabei_totalcreditamount}
    except Exception, e:
        edit_log("getAlipayHuabei error " + str(traceback.print_exc()))


# 获取收货地址
def getDeliveraddress():
    try:
        deliver_address_URL = 'http://member1.taobao.com/member/fresh/deliver_address.htm'
        global deliveraddress_arr
        global tb_cookies
        for i in range(3):
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(deliver_address_URL, headers=common.getBaseHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:
                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                pass
                        continue

                    base_Node = bs4.BeautifulSoup(r.text, 'lxml')
                    addr_node = base_Node.find_all("tr", attrs={'class': 'thead-tbl-address'})
                    if addr_node is None or len(addr_node) == 0:
                        continue
                    else:
                        for a in addr_node:
                            td_node = a.find_all("td")
                            name = td_node[0].get_text()
                            address = td_node[1].get_text().strip()
                            if address != '':
                                tmp = address.split(' ')
                                province = tmp[0]
                                city = tmp[1]
                                county = tmp[2] if len(tmp) > 2 else ''
                            else:
                                province = ''
                                city = ''
                                county = ''
                            full_address = td_node[2].get_text().strip()
                            zip_code = td_node[3].get_text()
                            phone_no = td_node[4].get_text().strip()
                            default = td_node[6].get_text() == u'默认地址'

                            deliveraddress_arr.append({
                                "name": name,
                                "address": address,
                                "province": province,
                                "city": city,
                                "county": county,
                                "full_address": full_address,
                                "zip_code": zip_code,
                                "phone_no": phone_no,
                                "default": default,
                            })

                        deliver_address = {
                            '_id': task_id,
                            'deliveraddress': deliveraddress_arr,
                            'nick': nickName,
                            'getdate': get_date()
                        }
                        insert_mongo(coll_deliver_address, deliver_address)
                        break
            except Exception, e:
                continue
    except Exception, e:
        edit_log("getDeliveraddress error" + str(traceback.print_exc()))


# 获取基本信息(会员名/邮箱/手机/真实姓名/支付宝账号/身份证号)
def getBaseInfo():
    bCookieTimeout = False
    try:
        global userinfo_obj
        global tb_cookies
        global task_id
        nick = ''
        email = ''
        phone_number = ''
        real_name = ''
        alipay_account = ''
        idNum = ''
        alipay_email = ''  # 页面改版，已失效
        alipay_bind_phone = ''  # 页面改版，已失效
        alipay_type = ''
        alipay_authname = ''
        alipay_authid = ''

        account_security_URL = 'https://member1.taobao.com/member/fresh/account_security.htm'
        account_profile_URL = 'https://member1.taobao.com/member/fresh/account_profile.htm'
        certify_info_URL = 'https://member1.taobao.com/member/fresh/certify_info.htm'
        account_management_URL = 'https://member1.taobao.com/member/fresh/account_management.htm?spm=a1z08.2.0.0.xrckCn'
        for i in range(3):  # 尝试3遍
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(account_security_URL, headers=common.getBaseHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:

                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                # edit_log('getBaseInfo: 302 err.taobao.com')
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                # edit_log('getBaseInfo: 302 login.taobao.com')
                                bCookieTimeout = True

                        continue
                    base_Node = bs4.BeautifulSoup(r.text, 'lxml')
                    info_node = base_Node.find_all("span", attrs={'class': 'default'})
                    if info_node is None or len(info_node) == 0:
                        # edit_log('getBaseInfo: info_node is None')
                        continue
                    else:
                        nick = info_node[0].get_text().strip()
                        # 如果未传入nick，把抓取的nick传回
                        global nickName
                        if not nickName:
                            nickName = nick
                        if len(info_node) == 4:
                            phone_number = info_node[3].get_text().strip()
                            email = info_node[2].get_text().strip()
                        else:
                            phone_number = info_node[2].get_text().strip()
                            email = info_node[1].get_text().strip()
                    break
            except Exception, e:
                # edit_log("getBaseInfo error: " + str(traceback.print_exc()))
                continue
        for i in range(3):  # 尝试3遍
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(account_profile_URL, headers=common.getBaseHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:

                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                bCookieTimeout = True
                        continue
                    base_Node = bs4.BeautifulSoup(r.text, 'lxml')
                    div_node = base_Node.find('div', class_='col-main')
                    realname_node = div_node.find("strong") if div_node is not None else None
                    if realname_node is None:
                        continue
                    real_name = realname_node.get_text() if realname_node is not None else ''
                    break
            except Exception, e:
                continue
        for i in range(3):
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(certify_info_URL, headers=common.getBaseHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:

                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                bCookieTimeout = True
                        continue

                    base_Node = bs4.BeautifulSoup(r.text, 'lxml')
                    idNum_node = base_Node.find_all("div", attrs={'class': 'left'})
                    if idNum_node is None or len(idNum_node) < 4:
                        continue
                    else:
                        idNum = idNum_node[3].get_text()

                    break
            except Exception, e:
                continue
        for i in range(3):
            try:
                with requests.Session() as s:
                    s.keep_alive = False
                    r = s.get(account_management_URL, headers=common.getBaseHeader(tb_cookies), allow_redirects=False,
                              verify=False, proxies=common.getProxy(), timeout=20)
                    if r.status_code is not 200:

                        if r.status_code == 302:
                            jumpUrl = r.headers['Location']
                            if 'err.taobao.com' in jumpUrl:
                                pass
                            elif 'login.taobao.com' in jumpUrl:
                                bCookieTimeout = True
                        continue

                    base_Node = bs4.BeautifulSoup(r.text, 'lxml')
                    alipayAccount_node = base_Node.find("span", attrs={'class': 'red'})
                    alipay_account = alipayAccount_node.get_text() if alipayAccount_node is not None else ''
                    if alipayAccount_node is None:
                        continue

                    alipay_email = ''
                    alipay_bind_phone = ''
                    alipay_type = ''
                    alipay_authname = ''
                    alipay_authid = ''
                    tb_node = base_Node.find("table", attrs={'class': 'table-list'})
                    if tb_node is not None:
                        td_nodes = tb_node.find_all("td")
                        if td_nodes is not None and len(td_nodes) != 0:
                            alipay_type = td_nodes[0].get_text().strip()
                            alipay_auth = td_nodes[2].get_text().strip().replace('&nbsp;', '')
                            authArr = alipay_auth.split('|')
                            alipay_authname = authArr[0].strip()
                            alipay_authid = authArr[1]
                            alipay_authid = alipay_authid[0:len(alipay_authid) - 3].strip()
                    break
            except Exception, e:
                continue

        if not bCookieTimeout:
            userinfo_obj = {
                '_id': task_id,
                'nick': nick,
                'email': email,
                'phone_number': phone_number,
                'real_name': real_name,
                'alipay_account': alipay_account,
                'alipay_email': alipay_email,
                'alipay_bind_phone': alipay_bind_phone,
                'alipay_type': alipay_type,
                'alipay_authname': alipay_authname,
                'alipay_authid': alipay_authid,
                'idNum': idNum,
                'getdate': get_date()
            }
            insert_mongo(coll_user_info, userinfo_obj)
        return True, bCookieTimeout
    except Exception, e:
        edit_log('getBaseInfo error ' + str(traceback.print_exc()))
        return False, bCookieTimeout


def start():
    try:
        # nick = ''   # 当前账户昵称
        # dateNow = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        global totalNumber
        totalNumber = 0

        global userinfo_obj
        userinfo_obj = {}

        global alipaywealth_obj
        alipaywealth_obj = {}

        global deliveraddress_arr
        deliveraddress_arr = []

        global tradeinfo_arr
        tradeinfo_arr = []

        global sellerinfo_arr
        sellerinfo_arr = []

        global task_id
        global loan_data
        global nickName
        global wsd_cookies
        global sycm_cookies

        is_user_info_succ = False  # 用户信息是否抓取成功
        wsd_data = {}  # 网商贷详细数据
        sycm_data = {}  # 生意参谋详细数据

        tmp_content = {
            # 淘宝订单的数据格式
            "tb": {
                "name": "",
                "baseMessageTBVo": {
                    "cardNum": "",
                    "email": "",
                    "phone": "",
                    "realName": "",
                    "tbName": ""
                },
                "bindingAddressTBVos": [],
                "collectAddressTBVos": [],
                "customerTaoBaoIndentVos": []
            },
            # 网商贷的数据格式
            "wsd": {

            },
            # 生意参谋的数据格式
            "sycm": {

            }
        }

        # 入库-基础信息
        b, bTimeout = getBaseInfo()
        if bTimeout:
            sycm_cookies = None
            # # cookie失效，生意参谋数据置空
            # sycm_data = {}
            # sycm_flag = False
            # sycm_msg = 'cookie timeout'

            # 往淘宝用户topic发送cookie过期信息
            # todo define errMsg, errCode
            notify_cookie_timeout = {
                "nick": nickName,
                "success": is_user_info_succ,
                "taskId": task_id,
                "userId": userId,
                "errMsg": "overdue",
                "errCode": "1"
            }
            kafka_client(
                notify_cookie_timeout,
                common.KAFKA_ADDRESS,
                common.KAFKA_PRODUCER_TB
            )

            # 往生意参谋topic发送cookie过期信息
            loan_data_overdue = get_loan_data(
                wsd_data={'flag': False, 'msg': 'overdue'},
                sycm_data={'flag': False, 'msg': 'overdue'},
            )
            kafka_client(
                loan_data_overdue,
                common.KAFKA_ADDRESS,
                common.KAFKA_PRODUCER_LOAN
            )
            return

        if userinfo_obj != {}:
            tmp_content["tb"]["name"] = userinfo_obj["alipay_authname"]
            tmp_content["tb"]["baseMessageTBVo"] = {}
            tmp_content["tb"]["baseMessageTBVo"]["cardNum"] = userinfo_obj["idNum"]
            tmp_content["tb"]["baseMessageTBVo"]["email"] = userinfo_obj["email"]
            tmp_content["tb"]["baseMessageTBVo"]["phone"] = userinfo_obj["phone_number"]
            tmp_content["tb"]["baseMessageTBVo"]["realName"] = userinfo_obj["alipay_authname"]
            tmp_content["tb"]["baseMessageTBVo"]["tbName"] = userinfo_obj["nick"]
        else:
            # edit_log('{nickName} get_baseInfo_fail'.format(nickName=nickName))
            pass
        userinfo_obj = {}

        # 入库-收货地址
        getDeliveraddress()
        if len(deliveraddress_arr) != 0:
            tmp_content["tb"]["bindingAddressTBVos"] = []
            for i in deliveraddress_arr:
                bindingAddressTBVo = {
                    "area": i["county"],
                    "city": i["city"],
                    "detail": i["full_address"],
                    "name": i["name"],
                    "phone": i["phone_no"],
                    "province": i["province"],
                    "zipCode": i["zip_code"]
                }
                tmp_content["tb"]["bindingAddressTBVos"].append(bindingAddressTBVo)
                is_user_info_succ = True
        else:
            # edit_log('{nickName} get_deliverAddress_fail'.format(nickName=nickName))
            pass
        deliveraddress_arr = []

        # 先从wap获取订单列表及其详情,若失败再尝试pc
        # if getOrderList() is False:
        #     getOrderList_FromPC()

        # 入库-订单列表(及详情)
        getOrderList_FromPC()
        if len(tradeinfo_arr) != 0:
            trade_info = {
                '_id': task_id,
                'totalcount': totalNumber,
                'actualcount': len(tradeinfo_arr),
                'nick': nickName,
                'tradeinfo': tradeinfo_arr,
                'getdate': get_date()
            }
            insert_mongo(coll_trade_info, trade_info)

            tmp_content["tb"]["collectAddressTBVos"] = []
            tmp_content["tb"]["customerTaoBaoIndentVos"] = []
            for i in tradeinfo_arr:
                try:
                    collectAddressTBVo = {
                        "name": i["deliverinfo"]["name"],
                        "number": i["sub_orders"][0]["quantity"],
                        "phone": i["deliverinfo"]["phone_no"],
                        "price": i["actual_fee"],
                        "shopName": i["sub_orders"][0]["item_name"],
                        "shopNickName": i["seller_shopname"],
                        "type": 0,
                        "createTime": i["trade_createtime"]
                    }
                    tmp_content["tb"]["collectAddressTBVos"].append(collectAddressTBVo)

                    customerTaoBaoIndentVo = {
                        "createTime": i["trade_createtime"],
                        "goodName": i["sub_orders"][0]["item_name"],
                        "money": i["actual_fee"]
                    }
                    tmp_content["tb"]["customerTaoBaoIndentVos"].append(customerTaoBaoIndentVo)
                    is_user_info_succ = True
                except IndexError:
                    # edit_log('{nickName} get_deliverAddress_fail'.format(nickName=nickName))
                    continue
        else:
            # edit_log('{nickName} get_deliverAddress_fail'.format(nickName=nickName))
            pass
        tradeinfo_arr = []

        # 发送用户信息抓取成功消息（kafka）
        notify_info_succ = {
            "nick": nickName,
            "success": is_user_info_succ,
            "taskId": task_id,
            "userId": userId,
            "errMsg": "",
            "errCode": ""
        }
        kafka_client(notify_info_succ, common.KAFKA_ADDRESS, common.KAFKA_PRODUCER_TB)

        # 抓取网商贷信息
        if wsd_cookies:
            print 'crawling wsd of %r' % nickName
            try:
                b_wsd, ret_wsd = tbLoan.run_tbLoan(nickName, wsd_cookies)
                wsd_data = ret_wsd if b_wsd else {}
                wsd_data['flag'] = b_wsd
                # wsd_data['msg'] = 'succ' if b_loan else ret_wsd
                if b_wsd:
                    wsd_data['msg'] = 'succ'
                elif ret_wsd == 'LoginRequired':
                    wsd_data['msg'] = 'unauthorized'
                else:
                    wsd_data['msg'] = 'unknown'
            except Exception as e:
                edit_log("wsd error: " + str(traceback.print_exc()))

        # 抓取生意参谋信息
        if sycm_cookies:
            print 'crawling sycm of %r' % nickName
            try:
                b_sycm, ret_sycm = sycm.run_sycm(nickName, sycm_cookies)
                sycm_data = ret_sycm if b_sycm else {}
                sycm_data['flag'] = b_sycm
                # wsd_data['msg'] = 'succ' if b_loan else ret_wsd
                if b_sycm:
                    sycm_data['msg'] = 'succ'
                elif ret_sycm == '302':
                    sycm_data['msg'] = 'unauthorized'
                else:
                    sycm_data['msg'] = 'unknown'
            except Exception as e:
                edit_log("sycm error: " + str(traceback.print_exc()))

        # 发送网商贷、生意参谋数据
        kafka_client(
            get_loan_data(wsd_data, sycm_data),
            common.KAFKA_ADDRESS,
            common.KAFKA_PRODUCER_LOAN
        )
    except Exception, e:
        edit_log("error: " + str(traceback.print_exc()))
        return
    return True


# 网商贷+生意参谋数据
def get_loan_data(wsd_data, sycm_data):
    if 'flag' not in wsd_data:
        wsd_data['flag'] = False
    if 'msg' not in wsd_data:
        wsd_data['msg'] = 'unknown'

    if 'flag' not in sycm_data:
        sycm_data['flag'] = False
    if 'msg' not in sycm_data:
        sycm_data['msg'] = 'unknown'
    return {
        "content": {
            "WSD": wsd_data,
            "SYCM": sycm_data
        },
        "nickName": nickName,
        "userId": userId,
        "serialNumber": serial_number
    }


def kafka_client(content, host_and_ports, topics):
    content = json.dumps(content, encoding="utf-8")  # 字典转json字符串（不能添加ensure_ascii=False，否则往卡夫卡传数据会报错）
    client = KafkaClient(hosts=host_and_ports)
    topic = client.topics[topics]
    with topic.get_sync_producer(max_request_size=1024 * 1024 * 10) as producer:
        producer.produce(content)
        pass
    edit_log("send msg to kafka:{content}".format(content=content))


def run():
    try:
        global task_id
        global userId
        global tb_cookies
        global wsd_cookies
        global sycm_cookies
        global nickName
        global serial_number

        while 1:
            # ret = r_client.spop(common.TRANSFER_STATION_NAME)
            # if not ret:
            #     time.sleep(5)
            #     continue
            # edit_log("spider get cookie:{msg}".format(msg=ret))

            try:
                # msg_json = json.loads(ret)
                # msg_json = json.loads(msg_json["body"])
                # tb_cookies = msg_json["content"][0]["TB"]  # 用于淘宝订单cookie
                # wsd_cookies = msg_json["content"][0]["WSD"] if "WSD" in msg_json["content"][0] else ""  # 用于网商贷cookie
                # sycm_cookies = msg_json["content"][0]["SYCM"] if "SYCM" in msg_json["content"][0] else ""  # 用于好管家cookie
                # nickName = msg_json["nickName"].encode("utf-8")
                # userId = msg_json["userId"].encode("utf-8")
                # serial_number = msg_json["serialNumber"]
                # task_id = str(uuid.uuid1())  # 任务id(作为mongdb的_id)

                # todo debug
                print 'start test cookie'
                tb_cookies = 'cna=7lG+EtvSmBoCAXDBkfcAb0eT; t=d04cf916dd35e9183288e1f4a9e54bcf; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; tg=0; ali_ab=112.193.145.247.1513653182632.0; UM_distinctid=1606cc29ed8e8-09dee1ac573fb5-5a442916-1fa400-1606cc29ed957e; l=AqOjlAGWoRwc5--sJKU1Cuwhs-xNnzfa; ucn=center; _m_h5_tk=17f5dacbe9eabf0e33b89cab2007f343_1513750253324; _m_h5_tk_enc=79150e90853cdc573f3ccc613881c67a; cookie2=10cdd8d9bc036d9f06b31fa485258be8; _tb_token_=eee9e7e5a3ea6; v=0; uc3=sg2=AighRir69sNs%2FWE%2Bz5ivR77Srh%2BuYnmj8QlwgviiYXM%3D&nk2=rPSn6Oxi&id2=VypTwv462fNq&vt3=F8dBzLbb2ppbs2yARpw%3D&lg2=W5iHLLyFOGW7aA%3D%3D; existShop=MTUxNDI2MDAxNA%3D%3D; uss=VFCqKu%2F4AvUJ44cJ8UpJlVee7xiQc2fhuudv7KkOMZHvLPkAWzXvcSsaUMA%3D; lgc=%5Cu6653%5Cu5C0F%5Cu7AF9; tracknick=%5Cu6653%5Cu5C0F%5Cu7AF9; sg=%E7%AB%B919; cookie1=VFJP3yqwmy8o0EvyPKwREfNhQJC5%2Fzb6MTVJl%2BPe4hE%3D; unb=463918261; skt=d2bb52c9918567a8; _cc_=U%2BGCWk%2F7og%3D%3D; _l_g_=Ug%3D%3D; _nk_=%5Cu6653%5Cu5C0F%5Cu7AF9; cookie17=VypTwv462fNq; mt=ci=0_1; uc1=cookie14=UoTdf1MGeDN%2F8A%3D%3D&lng=zh_CN&cookie16=UtASsssmPlP%2Ff1IHDsDaPRu%2BPw%3D%3D&existShop=false&cookie21=Vq8l%2BKCLjA%2Bl&tag=8&cookie15=W5iHLLyFOGW7aA%3D%3D&pas=0; isg=AkJCOdRKrFzusbB23yXRWyOnk0hku0LL_AXbxIxbwbVg3-NZdaEWPfJd-e1Y'
                wsd_cookies = None
                sycm_cookies = None
                nickName = 'test_by_hand'
                userId = 'test_by_hand'
                task_id = str(uuid.uuid1())
            except:
                # edit_log('cookie format error: {}'.format(ret))
                continue

            start()
            # todo debug
            # break
    except Exception, e:
        edit_log("error: {}".format(str(traceback.print_exc())))
        return run()


if __name__ == "__main__":
    run()
