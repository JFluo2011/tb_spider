#!/usr/bin/env python
# -*- coding:utf-8 -*-

""" 
@version: v1.0 
@author: yvon   
@software: PyCharm 
@file: sycm.py
@data: 2017/9/4 10:15
"""

# 说明:
# 抓取生意参谋(https://sycm.taobao.com),未使用代理IP
# 执行顺序: 淘宝订单及其他 -> 淘宝贷款 -> 生意参谋.
# 完成后通过http调用接口回传结果.需要抓取6部分指标:
#  1.日数据(excel)
#  2.流量来源(excel)
#  3.行业层级排名
#  4.交易趋势同行对比(excel)
#  5.交易类目构成(excel)
#  6.店铺扣分

from contextlib import closing
from pyquery import PyQuery as pq
from dateutil.relativedelta import relativedelta
import requests, json, datetime, calendar, urlparse, sys, traceback
from parseExcel_sycm import *
import common
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')
requests.packages.urllib3.disable_warnings()

# 生意参谋结果回调url
cbUrl = 'http://192.168.1.40:8080/stroeUploadData/uploadSycmData'
bLoginRequired = False
cookies = ''  # cookies = tb_lastest.cookies
istmall = False  # 是否天猫
sellerNick = ''  # 卖家昵称
firstDate = ''  # 月份起始
lastDate = ''  # 月份截止
sycmToken = ''  # token
batchStr = ''  # 天指标项id值
# todo edit retry
retry = 3  # 错误重试次数
excel_dir_name = 'excels'  # 存放excel的文件夹

# 全局结果
allExcelPath = []  # 全部excel文件path
industryCateArr = []  # 同行对比商品类别
dayObjArr = []  # 1.日表_结果(excel)
flowObjArr = []  # 2.流量来源_结果(excel)
industryranking = {}  # 3.行业层级排名_结果
tradetrendindustrycmprArr = []  # 4.交易趋势同业对比_结果(excel)
tradeCategoryArr = []  # 5.交易类目构成__结果(excel)
scoreObj = {}  # 6.店铺扣分_结果


# 装饰器,用于统一判断登录
def checkLogin(func):
    def wrapper(*args, **kwargs):
        global bLoginRequired
        if bLoginRequired:
            print 'LoginRequired,Exit !!'
            return False, 'LoginRequired !!'
        else:
            return func(*args, **kwargs)

    return wrapper


# 1.日数据(excel)
@checkLogin
def downDayExcel():
    try:
        # sycm_d
        now = datetime.datetime.now()
        global dayObjArr, allExcelPath, batchStr

        # 说明: 每个Excel包含年初到年尾的数据;取至少2年
        for i in range(retry):
            fromDate = datetime.datetime(now.year - i, 1, 1).strftime('%Y-%m-%d')
            endDate = datetime.datetime(now.year - i, 12, 31).strftime('%Y-%m-%d')

            url = 'https://sycm.taobao.com/adm/export.do?dateType=static&dateId=1006960&owner=user&filter=[6,7]&app=op&show=' + batchStr + '&date=' + fromDate + ',' + endDate
            excelName = fromDate + '_' + endDate + '_day'
            b, filepath, errMsg = downloadFile(url, excelName)
            if b is False:
                common.printWarning('downlodDayExcel fail: ' + errMsg)
                return False, errMsg

            allExcelPath.append(filepath)

            # 解析Excel
            b, dayObjArr_year, errMsg = parseDayExcel(filepath)
            if b is False:
                common.printWarning('downlodDayExcel parse_fail: ' + errMsg)
                return False, errMsg

            if dayObjArr_year:
                dayObjArr.extend(dayObjArr_year)
        return True, errMsg
    except Exception, e:
        traceback.print_exc()
        errMsg = 'downlodDayExcel exception: ' + str(e)
        common.printWarning(errMsg)
        return False, errMsg


# 2.流量来源_逐月(excel)
@checkLogin
def downFlowmapExcel():
    try:
        # sycm_flowmap
        global firstDate, lastDate, allExcelPath, flowObjArr
        DownPcFlow_URL = "https://sycm.taobao.com/bda/download/excel/flow/flowmap/FlowSourceExcel.do?sourceDataType=0&cateId=0&dateRange=" + firstDate + "|" + lastDate + "&dateType=month&device=1&deviceLogicType=1&index=uv,orderBuyerCnt,orderRate"
        DownWirelessFlow_URL = "https://sycm.taobao.com/bda/download/excel/flow/flowmap/FlowSourceExcel.do?sourceDataType=0&dateRange=" + firstDate + "|" + lastDate + "&dateType=month&device=4&deviceLogicType=2&index=uv,orderBuyerCnt,orderRate"

        excelName = firstDate + '_' + lastDate
        b, filepath1, errMsg = downloadFile(DownPcFlow_URL, excelName + '_flowsource_pc')
        if b is False:
            common.printWarning('downFlowmapExcel fail: ' + errMsg)
            return False, errMsg

        excelName = firstDate + '_' + lastDate
        b, filepath2, errMsg = downloadFile(DownWirelessFlow_URL, excelName + '_flowsource_wl')
        if b is False:
            common.printWarning('downFlowmapExcel fail: ' + errMsg)
            return False, errMsg

        # terminalType: 1:PC 2:无线
        fileArr = [filepath1, filepath2]
        for terminalType in range(len(fileArr)):
            filepath = fileArr[terminalType]
            allExcelPath.append(filepath)
            b, flowObjArr_month, errMsg = parseFlowmapExcel(filepath, firstDate, terminalType)
            if b is False:
                common.printWarning('downFlowmapExcel parse_fail: ' + errMsg)
                return False, errMsg

            if flowObjArr_month:
                flowObjArr.extend(flowObjArr_month)
        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = 'downFlowmapExcel exception: ' + str(e)
        common.printWarning(errMsg)
        return False, errMsg


# 3.行业层级排名
@checkLogin
def getIndustryranking():
    global istmall
    # sycm_industryranking
    url = "https://sycm.taobao.com/portal/rank/getShopRank.json?_=" + common.getDate13()
    b, html, errMsg = makeRequest(url)

    # 判断是否为天猫店铺
    if 'shopType' in html:
        shopType = json.loads(html)['content']['data']['shopType']
        istmall = False if shopType == 0 else True
    else:
        errMsg = '获取店铺类型失败'
        return False, errMsg

    if b is False:
        common.printWarning('getIndustryranking fail: ' + errMsg)
        return False, errMsg

    try:
        # {"content":{"traceId":"0ab54a6215045898326436016e4fe2","message":"操作成功","data":{"levelMap":{"1":"0.0元","2":"900.0元","3":"1.2万元","4":"4.9万元","5":"14.6万元","6":"54.6万元"},"cateLevelYesterday":1,"isCateChange":false,"rankCrc":-2,"cateLevel":1,"shopType":0,"cateId":50014811,"cateName":"网店\/网络服务\/软件","trend":[{"level":1,"statDate":1501948800000,"rate":0.0,"rank":25558},{"level":1,"statDate":1502035200000,"rate":0.0,"rank":25494},{"level":1,"statDate":1502121600000,"rate":0.0,"rank":25435},{"level":1,"statDate":1502208000000,"rate":0.0,"rank":25275},{"level":1,"statDate":1502294400000,"rate":0.0,"rank":25117},{"level":1,"statDate":1502380800000,"rate":0.0,"rank":24910},{"level":1,"statDate":1502467200000,"rate":0.0,"rank":24705},{"level":1,"statDate":1502553600000,"rate":0.0,"rank":24474},{"level":1,"statDate":1502640000000,"rate":0.0,"rank":24450},{"level":1,"statDate":1502726400000,"rate":0.0,"rank":24353},{"level":1,"statDate":1502812800000,"rate":0.0,"rank":24106},{"level":1,"statDate":1502899200000,"rate":0.0,"rank":23782},{"level":1,"statDate":1502985600000,"rate":0.0,"rank":23581},{"level":1,"statDate":1503072000000,"rate":0.0,"rank":23367},{"level":1,"statDate":1503158400000,"rate":0.0,"rank":23121},{"level":1,"statDate":1503244800000,"rate":0.0,"rank":23043},{"level":1,"statDate":1503331200000,"rate":0.0,"rank":22942},{"level":1,"statDate":1503417600000,"rate":0.0,"rank":22720},{"level":1,"statDate":1503504000000,"rate":0.0,"rank":22536},{"level":1,"statDate":1503590400000,"rate":0.0,"rank":22332},{"level":1,"statDate":1503676800000,"rate":0.0,"rank":22215},{"level":1,"statDate":1503763200000,"rate":0.0,"rank":22089},{"level":1,"statDate":1503849600000,"rate":0.0,"rank":22069},{"level":1,"statDate":1503936000000,"rate":0.0,"rank":22067},{"level":1,"statDate":1504022400000,"rate":0.0,"rank":22006},{"level":1,"statDate":1504108800000,"rate":0.0,"rank":21862},{"level":1,"statDate":1504195200000,"rate":0.0,"rank":21720},{"level":1,"statDate":1504281600000,"rate":0.0,"rank":21652},{"level":1,"statDate":1504368000000,"rate":0.0,"rank":21500},{"level":1,"statDate":1504454400000,"rate":0.0,"rank":21502}],"pictureUrl":"\/\/img.alicdn.com\/imgextra\/d4\/4a\/TB1Oe3iRFXXXXXAapXXSutbFXXX.jpg","shopTitle":"d[s129775901]","rank":21502},"code":0},"hasError":false}
        jsonObj = json.loads(html)
        if jsonObj.has_key('code'):
            errMsg = jsonObj['msg'] if jsonObj.has_key('msg') else 'result json is wrong'
            common.printWarning('getIndustryranking fail: ' + errMsg)
            return False, errMsg
        if jsonObj is None or jsonObj['hasError'] is True:
            errMsg = 'result json wrong'
            common.printWarning('getIndustryranking fail: ' + errMsg)
            return False, errMsg

        level = jsonObj['content']['data']['cateLevel']
        rank = jsonObj['content']['data']['rank']
        cateName = jsonObj['content']['data']['cateName'].encode('utf8')

        global industryranking
        industryranking = {'level': level, 'rank': rank, 'category': cateName}
        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getIndustryranking exception: ' + errMsg)
        return False, errMsg


# 4.交易趋势同行对比_逐月(excel)
@checkLogin
def downTradetrendindustrycpr():
    try:
        # sycm_tradetrendindustrycomp
        global firstDate, lastDate, industryCateArr, allExcelPath, tradetrendindustrycmprArr

        for cate in industryCateArr:
            categoryCode = str(cate['industryId'])
            # comprType: 2:同行优秀/1:同行平均/3:同行比较/0:不比较
            for comprType in range(1, 4):
                comprTypeStr = str(comprType)
                url = "https://sycm.taobao.com/bda/download/excel/tradinganaly/overview/TendencyExcel.do?dateType=month&categoryId=" + categoryCode + "&diff=" + comprTypeStr + "&dateRange=" + firstDate + "|" + lastDate
                excelName = "categoryid-" + categoryCode + "_type-" + comprTypeStr + "_" + firstDate + "_" + lastDate + '_tradetrendindcpr'
                b, filepath, errMsg = downloadFile(url, excelName)
                if b is False:
                    common.printWarning('downTradetrendindustrycpr fail: ' + errMsg)
                    continue

                allExcelPath.append(filepath)

                # 解析Excel
                b, tradetrendindustrycprObjArr, errMsg = parseTradetrendindustrycprExcel(filepath, categoryCode,
                                                                                         comprTypeStr)
                if b is False:
                    common.printWarning('downTradetrendindustrycpr parse_fail: ' + errMsg)
                    return False, errMsg

                if tradetrendindustrycprObjArr:
                    tradetrendindustrycmprArr.extend(tradetrendindustrycprObjArr)

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('downTradetrendindustrycpr exception: ' + errMsg)
        return False, errMsg


# 5.交易类目构成_逐月(excel)
@checkLogin
def downTradeCategoryExcel():
    try:
        # sycm_categorydetail
        global firstDate, lastDate, allExcelPath, tradeCategoryArr
        ConstituteCategoryExcel_URL = "https://sycm.taobao.com/bda/download/excel/tradinganaly/constitute/CategoryExcel.do?dateType=month&dateRange=" + firstDate + "|" + lastDate

        excelName = firstDate + "_" + lastDate + '_tradecategory'
        b, filepath, errMsg = downloadFile(ConstituteCategoryExcel_URL, excelName)
        if b is False:
            common.printWarning('downTradeCategoryExcel fail: ' + errMsg)
            return False, errMsg

        allExcelPath.append(filepath)

        # 解析Excel
        b, tradeCategoryObjArr, errMsg = parseTradeCategoryExcel(filepath, firstDate)
        if b is False:
            common.printWarning('downTradeCategoryExcel parse_fail: ' + errMsg)
            return False, errMsg

        if tradeCategoryObjArr:
            tradeCategoryArr.extend(tradeCategoryObjArr)
        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('downTradeCategoryExcel exception: ' + errMsg)
        return False, errMsg


# 6.店铺扣分
@checkLogin
def getDeductscore():
    # sycm_deductscore
    global istmall
    # url = 'https://sapp.taobao.com/tmallwork/transSellerRemind.htm?tmallcenter=true&spm=687.8433302.40003.i1' if istmall else 'https://myseller.taobao.com/ajax/widget.do?name=seller_todo_list&t='   # tmall,taobao 接口均失效
    url = 'https://zhaoshang.tmall.com/tmallwork/transSellerRemind.htm?tmallcenter=true&spm=687.8433302.40003.i1' if istmall else 'https://myseller.taobao.com/ajaxProxy.do?action=widgetSellerTodoListAction&event_submit_do_service=true'
    b, html, errMsg = makeRequest(url) if istmall else makeRequest(url)
    if b is False:
        common.printWarning('getBatchList fail: ' + errMsg)
        return False, errMsg

    try:
        global scoreObj
        doc = pq(html)
        soup = BeautifulSoup(html, 'lxml')
        if istmall:
            # 解析有误
            # scoreNode = doc('a[href*=punish_history]')
            # if scoreNode is None or len(scoreNode) == 0:
            #     errMsg = 'a[href*=punish_history] empty'
            #     common.printWarning('getDeductscore fail: ' + errMsg)
            #     return False, errMsg

            scoreNode = list()
            if 'punish_history' in html:
                for a_node in soup.find_all('a'):
                    if 'punish_history' in a_node['href']:
                        scoreNode.append(a_node)
            scoreObj['normal_illegal'] = scoreNode[1].text.strip() if scoreNode[1].text else ''
            scoreObj['serious_illegal'] = scoreNode[2].text.strip() if scoreNode[2].text else ''

        else:
            # 解析失效 2017/11/27
            # titleNodes = doc('span.todo-item-title')
            # valueNodes = doc('span.todo-item-value')
            # if titleNodes is None or len(titleNodes) == 0:
            #     errMsg = 'span.todo-item-title empty'
            #     common.printWarning('getDeductscore fail: ' + errMsg)
            #     return False, errMsg
            #
            # if valueNodes is None or len(valueNodes) == 0:
            #     errMsg = 'span.todo-item-value empty'
            #     common.printWarning('getDeductscore fail: ' + errMsg)
            #     return False, errMsg
            #
            # for i in range(len(titleNodes)):
            #     if titleNodes[i].text.__contains__(u"一般违规累计扣分"):
            #         scoreObj['normal_illegal_score'] = valueNodes[i].text.strip()
            #     elif titleNodes[i].text.__contains__(u"严重违规累计扣分"):
            #         scoreObj['serious_illegal_score'] = valueNodes[i].text.strip()
            #     elif titleNodes[i].text.__contains__(u"售假违规累计扣分"):
            #         scoreObj['fake_illegal_score'] = valueNodes[i].text.strip()  # tb only

            jdata = json.loads(html)
            if 'data' not in jdata:
                errMsg = 'deduct-score "data" field is empty'
                common.printWarning('getDeductscore fail: ' + errMsg)
                return False, errMsg
            if 'shopBusiness' not in jdata['data']:
                errMsg = 'deduct-score "shopBusiness" field is empty'
                common.printWarning('getDeductscore fail: ' + errMsg)
                return False, errMsg
            for item in jdata['data']['shopBusiness']:
                if item['title'] == '一般违规累计扣分':
                    scoreObj['normal_illegal_score'] = item['value']
                if item['title'] == '严重违规累计扣分':
                    scoreObj['serious_illegal_score'] = item['value']
                if item['title'] == '售假违规累计扣分':
                    scoreObj['fake_illegal_score'] = item['value']

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getDeductscore exception: ' + errMsg)
        return False, errMsg


# 获取生意参谋token
@checkLogin
def getToken():
    url = 'https://sycm.taobao.com/portal/home.htm'
    b, html, errMsg = makeRequest(url)
    if b is False:
        common.printWarning('getToken fail: ' + errMsg)
        return False, errMsg

    try:
        doc = pq(html)
        microdataNode = doc('meta[name="microdata"]')
        if microdataNode is None or len(microdataNode) == 0:
            errMsg = 'meta[name="microdata"] empty'
            common.printWarning('getToken fail: ' + errMsg)
            return False, errMsg
        attrArr = microdataNode[0].attrib['content'].split(';')

        global sycmToken
        for item in attrArr:
            # viewMode=;storeVersion=201708281108;isSudo=0;ctxEnv=online;bucUserId=;bucUserName=;loginUserId=2483207872;loginUserName=;mainUserId=2483207872;mainUserName=;runAsUserId=2483207872;runAsUserName=;runAsShopType=;runAsShopTitle=;runAsShopId=;isSudo=0;legalityToken=c8c299282
            if item.__contains__("token=") or item.__contains__("legalityToken="):
                sycmToken = item.split('=')[1]
                return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getToken exception: ' + errMsg)
        return False, errMsg


# 获取同行对比商品类别
@checkLogin
def getTradeCategory():
    url = "https://sycm.taobao.com/bda/flow/flowsummary/flow_summary.htm?_res_id_=46"
    b, html, errMsg = makeRequest(url)
    if b is False:
        common.printWarning('getTradeCategory fail: ' + errMsg)
        return False, errMsg

    try:
        doc = pq(html)
        articleNode = doc('article[id="main"]')
        if articleNode is None or len(articleNode) == 0:
            errMsg = 'article[id="main"] not found'
            common.printWarning('getTradeCategory fail: ' + errMsg)
            return False, errMsg

        jsonStr = articleNode[0].attrib['data-industry-infos'].strip().encode('utf8')
        tmp = json.loads(jsonStr)
        global industryCateArr
        for j in tmp:
            industryCateArr.append({'industryId': j['industryId'], 'industryName': j['industryName'].encode('utf8')})

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = 'getTradeCategory exception: ' + str(e)
        common.printWarning(errMsg)
        return False, errMsg


# 获取天指标项
@checkLogin
def getBatchList4day():
    # 固定id值
    # global batchStr
    # batchStr = "[{'id':1006966},{'id':1014476},{'id':1014477},{'id':1014506},{'id':1014507},{'id':1011650},{'id':1006964},{'id':1007696},{'id':1016534},{'id':1007108},{'id':1007557},{'id':1016014},{'id':1014594},{'id':1016030},{'id':1006965},{'id':1007695},{'id':1016536},{'id':1006973},{'id':1014508},{'id':1014509},{'id':1014510},{'id':1006971},{'id':1007117},{'id':1007566},{'id':1006972},{'id':1007572},{'id':1006976},{'id':1006974},{'id':1006975},{'id':1007571},{'id':1007110},{'id':1007111},{'id':1016053},{'id':1014481},{'id':1007113},{'id':1016771},{'id':1007114},{'id':1007116},{'id':1007115},{'id':1016040},{'id':1007118},{'id':1007563},{'id':1006969},{'id':1016007},{'id':1016010},{'id':1014478},{'id':1006967},{'id':1006968},{'id':1016432},{'id':1014479},{'id':1011647},{'id':1011648},{'id':1014515},{'id':1014514},{'id':1014494},{'id':1014492},{'id':1014493},{'id':1016035},{'id':1014517},{'id':1014516},{'id':1014497},{'id':1014495},{'id':1014496},{'id':1016034},{'id':1007562},{'id':1014519},{'id':1014518},{'id':1014499},{'id':1014498},{'id':1014538},{'id':1016055},{'id':1016773},{'id':1006980},{'id':1014512},{'id':1014513},{'id':1006978},{'id':1016011},{'id':1007126},{'id':1007575},{'id':1006979},{'id':1007581},{'id':1006984},{'id':1006981},{'id':1006982},{'id':1007580},{'id':1007119},{'id':1007120},{'id':1016017},{'id':1014482},{'id':1007122},{'id':1016043},{'id':1016772},{'id':1007123},{'id':1016050},{'id':1007125},{'id':1007124},{'id':1016031},{'id':1007127},{'id':1014486},{'id':1007101},{'id':1007102},{'id':1014490},{'id':1014491},{'id':1011898},{'id':1014489},{'id':1007103},{'id':1016045},{'id':1014483},{'id':1007558},{'id':1016022},{'id':1011649},{'id':1014480},{'id':1007104},{'id':1008451},{'id':1007105},{'id':1007107},{'id':1007106},{'id':1016013},{'id':1007109}]"
    # return True, None

    global sycmToken
    url = 'https://sycm.taobao.com/adm/field/batchList.json?app=op&dim=[%2210,6,7%22,%2211,6,7%22,%2214,6,7%22,%2215,6,7%22]&sycmToken=' + sycmToken
    b, html, errMsg = makeRequest(url)
    if b is False:
        common.printWarning('getBatchList fail: ' + errMsg)
        return False, errMsg

    try:
        jsonObj = json.loads(html)
        if jsonObj is None or jsonObj['code'] != 0:
            errMsg = jsonObj['msg'] if jsonObj.has_key('msg') else 'result json is wrong'
            common.printWarning('getBatchList fail: ' + errMsg)
            return False, errMsg

        global batchStr
        batchStr = '['
        for op in jsonObj['data']:
            if op['id'] == 1006960:
                continue
            batchStr += "{'id':" + str(op['id']) + "},"

            # 指标与id对应情况:
            # [{'id':1006966,'name':'被浏览商品数'},{'id':1014476,'name':'店铺收藏次数'},{'id':1014477,'name':'店铺收藏人数'},{'id':1014506,'name':'店铺首页访客数'},{'id':1014507,'name':'店铺首页浏览量'},{'id':1011650,'name':'DSR综合低评分买家数'},{'id':1006964,'name':'访客数'},{'id':1007696,'name':'访客数较前一天变化量'},{'id':1016534,'name':'服务态度动态评分(DSR)'},{'id':1007108,'name':'客单价'},{'id':1007557,'name':'老访客数'},{'id':1016014,'name':'老访客数占比'},{'id':1014594,'name':'老买家数'},{'id':1016030,'name':'老买家数占比'},{'id':1006965,'name':'浏览量'},{'id':1007695,'name':'浏览量较前一天变化量'},{'id':1016536,'name':'描述相符动态评分(DSR)'},{'id':1006973,'name':'PC端被浏览商品数'},{'id':1014508,'name':'PC端店铺首页访客数'},{'id':1014509,'name':'PC端店铺首页浏览量'},{'id':1014510,'name':'PC端店铺首页平均停留时长'},{'id':1006971,'name':'PC端访客数'},{'id':1007117,'name':'PC端客单价'},{'id':1007566,'name':'PC端老访客数'},{'id':1006972,'name':'PC端浏览量'},{'id':1007572,'name':'PC端人均浏览量'},{'id':1006976,'name':'PC端人均停留时长'},{'id':1006974,'name':'PC端商品详情页访客数'},{'id':1006975,'name':'PC端商品详情页浏览量'},{'id':1007571,'name':'PC端跳失率'},{'id':1007110,'name':'PC端下单金额'},{'id':1007111,'name':'PC端下单买家数'},{'id':1016053,'name':'PC端下单转化率'},{'id':1014481,'name':'PC端支付父订单数'},{'id':1007113,'name':'PC端支付金额'},{'id':1016771,'name':'PC端支付老买家数'},{'id':1007114,'name':'PC端支付买家数'},{'id':1007116,'name':'PC端支付商品件数'},{'id':1007115,'name':'PC端支付商品数'},{'id':1016040,'name':'PC端支付转化率'},{'id':1007118,'name':'PC端支付子订单数'},{'id':1007563,'name':'人均浏览量（访问深度）'},{'id':1006969,'name':'人均停留时长'},{'id':1016007,'name':'人均支付商品件数'},{'id':1016010,'name':'人均支付子订单数'},{'id':1014478,'name':'商品收藏次数'},{'id':1006967,'name':'商品详情页访客数'},{'id':1006968,'name':'商品详情页浏览量'},{'id':1016432,'name':'商品详情页支付转化率'},{'id':1014479,'name':'商品收藏人数'},{'id':1011647,'name':'售中申请退款金额'},{'id':1011648,'name':'售中申请退款买家数'},{'id':1014515,'name':'手机淘宝APP访客数'},{'id':1014514,'name':'手机淘宝APP浏览量'},{'id':1014494,'name':'手机淘宝APP支付金额'},{'id':1014492,'name':'手机淘宝APP支付买家数'},{'id':1014493,'name':'手机淘宝APP支付商品件数'},{'id':1016035,'name':'淘宝App支付转化率'},{'id':1014517,'name':'天猫App访客数'},{'id':1014516,'name':'天猫App浏览量'},{'id':1014497,'name':'天猫App支付金额'},{'id':1014495,'name':'天猫App支付买家数'},{'id':1014496,'name':'天猫APP支付商品件数'},{'id':1016034,'name':'天猫App支付转化率'},{'id':1007562,'name':'跳失率'},{'id':1014519,'name':'WAP访客数'},{'id':1014518,'name':'WAP浏览量'},{'id':1014499,'name':'WAP支付金额'},{'id':1014498,'name':'WAP支付买家数'},{'id':1014538,'name':'WAP支付商品件数'},{'id':1016055,'name':'Wap支付转化率'},{'id':1016773,'name':'物流服务动态评分(DSR)'},{'id':1006980,'name':'无线端被浏览商品数'},{'id':1014512,'name':'无线端店铺首页访客数'},{'id':1014513,'name':'无线端店铺首页浏览量'},{'id':1006978,'name':'无线端访客数'},{'id':1016011,'name':'无线端访客数占比'},{'id':1007126,'name':'无线端客单价'},{'id':1007575,'name':'无线端老访客数'},{'id':1006979,'name':'无线端浏览量'},{'id':1007581,'name':'无线端人均浏览量'},{'id':1006984,'name':'无线端人均停留时长'},{'id':1006981,'name':'无线端商品详情页访客数'},{'id':1006982,'name':'无线端商品详情页浏览量'},{'id':1007580,'name':'无线端跳失率'},{'id':1007119,'name':'无线端下单金额'},{'id':1007120,'name':'无线端下单买家数'},{'id':1016017,'name':'无线端下单转化率'},{'id':1014482,'name':'无线端支付父订单数'},{'id':1007122,'name':'无线端支付金额'},{'id':1016043,'name':'无线端支付金额占比'},{'id':1016772,'name':'无线端支付老买家数'},{'id':1007123,'name':'无线端支付买家数'},{'id':1016050,'name':'无线端支付买家数占比'},{'id':1007125,'name':'无线端支付商品件数'},{'id':1007124,'name':'无线端支付商品数'},{'id':1016031,'name':'无线端支付转化率'},{'id':1007127,'name':'无线端支付子订单数'},{'id':1014486,'name':'下单父订单数'},{'id':1007101,'name':'下单金额'},{'id':1007102,'name':'下单买家数'},{'id':1014490,'name':'下单且支付父订单数'},{'id':1014491,'name':'下单且支付金额'},{'id':1011898,'name':'下单且支付买家数'},{'id':1014489,'name':'下单且支付商品件数'},{'id':1007103,'name':'下单商品件数'},{'id':1016045,'name':'下单转化率'},{'id':1014483,'name':'下单子订单数'},{'id':1007558,'name':'新访客数'},{'id':1016022,'name':'新买家数'},{'id':1011649,'name':'已发货父订单数'},{'id':1014480,'name':'支付父订单数'},{'id':1007104,'name':'支付金额'},{'id':1008451,'name':'支付金额较前一天变化量'},{'id':1007105,'name':'支付买家数'},{'id':1007107,'name':'支付商品件数'},{'id':1007106,'name':'支付商品数'},{'id':1016013,'name':'支付转化率'},{'id':1007109,'name':'支付子订单数'}]
            # batchStr += "{'id':" + str(op['id']) + ",'name':'" + op['name'].encode('utf8') + "'},"
        batchStr = batchStr[:-1]
        batchStr += ']'

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = 'getBatchList exception: ' + str(e)
        common.printWarning(errMsg)
        return False, errMsg


# 生成过去2年内的月份起止时间
@checkLogin
def getMonths():
    try:
        now = datetime.datetime.now()
        fromDate = datetime.datetime(now.year - 2, now.month, 1)

        dictMonth = []
        for i in range(24):
            firstDayWeekDay, monthRange = calendar.monthrange(fromDate.year, fromDate.month)
            firstDate = datetime.date(year=fromDate.year, month=fromDate.month, day=1)
            lastDate = datetime.date(year=fromDate.year, month=fromDate.month, day=monthRange)
            fromDate += relativedelta(months=1)
            # print str(firstDay) + ' / ' + str(lastDay)
            dictMonth.append({'firstDate': str(firstDate), 'lastDate': str(lastDate)})
        return True, dictMonth
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getMonths exception: ' + errMsg)
        return False, errMsg


# 生成header
def getSycmheader(url):
    global cookies
    sycm_hearder = {
        'Host': urlparse.urlparse(url).netloc,
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cookie': cookies,
        'referer': 'https://myseller.taobao.com/home.htm'
    }
    return sycm_hearder


# 生成请求
def makeRequest(url):
    for i in range(retry):
        errMsg = ''
        try:
            # r = requests.get(url, headers=getSycmheader(url), allow_redirects=False, verify=False, timeout=25)
            r = requests.get(url, headers=getSycmheader(url), allow_redirects=False, verify=False,
                             proxies=common.getProxy(), timeout=25)
            if r.status_code != 200:
                if r.status_code == 302 and r.headers['Location'].__contains__('login'):
                    global bLoginRequired
                    bLoginRequired = True
                    errMsg = 'You must login system first.'
                else:
                    errMsg = str(r.status_code)
                continue
            elif r.text == '':
                errMsg = 'r.text is empty'
                continue
            html = r.text
            return True, html, errMsg
        except Exception, e:
            traceback.print_exc()
            errMsg = 'makeRequest Exception' + str(e)
            print url
            continue
    return False, None, errMsg


# 下载文件
def downloadFile(url, excelName=''):
    errMsg = 'succ'
    chunk_size = 1024
    # filepath = excelName + '_' + str(uuid.uuid4())[:8] + '.xls'

    global sellerNick
    filepath = excel_dir_name + '/' + excelName + '_' + sellerNick.replace(' ', '') + '.xls'

    for i in range(retry):
        if i == retry:
            proxies = common.getProxy()
        else:
            proxies = None
        try:
            with closing(requests.get(url, stream=True, headers=getSycmheader(url), allow_redirects=False, verify=False,
                                      timeout=25, proxies=proxies)) as r:
                # with closing(requests.get(url, stream=True, headers=getSycmheader(url), allow_redirects=False, verify=False,
                #                          proxies=common.getProxy(), timeout=25)) as response:

                if r.status_code != 200:
                    if r.status_code == 302 and r.headers['Location'].__contains__('login'):
                        global bLoginRequired
                        bLoginRequired = True
                        errMsg = 'You must login system first.'
                    else:
                        errMsg = str(r.status_code)
                    return False, None, errMsg

                with open(filepath, "wb") as f:
                    for data in r.iter_content(chunk_size=chunk_size):
                        f.write(data)

            return True, filepath, errMsg
        except Exception, e:
            traceback.print_exc()
            errMsg = 'downloadFile Exception' + str(e)
            continue
    return False, None, errMsg


def postSycmResult(resultJson):
    global cbUrl
    errMsg = 'postSycmResult fail'
    for i in range(3):
        try:
            r = requests.post(cbUrl, data=json.dumps(resultJson),
                              headers={'Content-Type': 'application/json;charset=UTF-8'}, timeout=40)
            # {"obj":null,"status":"0","code":null,"message":"处理成功","totalCount":0}
            rspJson = json.loads(r.text)
            if r.status_code != 200:
                return False, 'postSycmResult fail: ' + str(r.status_code)
            elif rspJson['status'] != "0":
                return False, 'postSycmResult fail: ' + rspJson['message']

            return True, None
        except Exception, e:
            traceback.print_exc()
            errMsg = 'postSycmResult Exception: ' + str(e)
            continue
    return False, errMsg


# 执行生意参谋抓取
def run_sycm(sellernick_, cookies_):
    try:
        global bLoginRequired, cookies, allExcelPath, industryCateArr, dayObjArr, flowObjArr \
            , industryranking, tradetrendindustrycmprArr, tradeCategoryArr, scoreObj, istmall, sellerNick
        bLoginRequired = False
        allExcelPath = []  # 全部excel文件path
        industryCateArr = []  # 同行对比商品类别
        dayObjArr = []  # 1.日表_结果(excel)
        flowObjArr = []  # 2.流量来源_结果(excel)
        industryranking = {}  # 3.行业层级排名_结果
        tradetrendindustrycmprArr = []  # 4.交易趋势同业对比_结果(excel)
        tradeCategoryArr = []  # 5.交易类目构成__结果(excel)
        scoreObj = {}  # 6.店铺扣分_结果

        cookies = cookies_
        sellerNick = sellernick_

        # 注意: 需要在原淘宝cookie基础上增加,生意参谋(流量部分)才能正确拉取 !!
        cookies += ';flow_version=old'

        resultJsonFail = {
            "flag": False,
            "msg": "fail",
            "sellerNick": sellerNick,
            "getDate": time.strftime("%Y-%m-%d", time.localtime()),
            "day": [],  # 1.日表(excel)
            "flow": [],  # 2.来源流量(excel)
            "industryRanking": {},  # 3.行业层级排名
            "tradeTrendIndustryCmpr": [],  # 4.交易趋势同业对比(excel)
            "tradeCategory": [],  # 5.交易类目构成(excel)
            "deductScore": {}  # 6.店铺扣分
        }

        # 创建文件夹，存储xcel
        import os
        import shutil
        if os.path.exists(excel_dir_name):
            shutil.rmtree(excel_dir_name)
        os.mkdir(excel_dir_name)

        b, errMsg = getToken()  # token
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postSycmResult(resultJsonFail)
            return b, errMsg

        b, errMsg = getIndustryranking()  # 3.行业层级排名
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postSycmResult(resultJsonFail)
            return b, errMsg

        b, errMsg = getBatchList4day()  # 获取天指标项
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postSycmResult(resultJsonFail)
            return b, errMsg

        b, errMsg = downDayExcel()  # 1.日数据(excel)
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postSycmResult(resultJsonFail)
            return b, errMsg

        b, errMsg = getTradeCategory()  # 获取同行对比商品类别
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postSycmResult(resultJsonFail)
            return b, errMsg

        b, errMsg = getDeductscore()  # 6.店铺扣分
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postSycmResult(resultJsonFail)
            return b, errMsg

        # 部分数据以月为单位,因此遍历2年内所有月份,逐一请求
        b, dictMonth = getMonths()
        if b:
            global firstDate, lastDate
            for dt in dictMonth:
                firstDate = dt['firstDate']
                lastDate = dt['lastDate']
                b, errMsg = downFlowmapExcel()  # 2.流量来源_逐月(excel)
                if b is False:
                    resultJsonFail['msg'] = errMsg
                    # postSycmResult(resultJsonFail)
                    return b, errMsg

                b, errMsg = downTradetrendindustrycpr()  # 4.交易趋势同行对比_逐月(excel)
                if b is False:
                    resultJsonFail['msg'] = errMsg
                    # postSycmResult(resultJsonFail)
                    return b, errMsg

                b, errMsg = downTradeCategoryExcel()  # 5.交易类目构成_逐月(excel)
                if b is False:
                    resultJsonFail['msg'] = errMsg
                    # postSycmResult(resultJsonFail)
                    return b, errMsg

        # 打包压缩全部excel文件存档
        # global sellerNick
        # zipFileName = sellerNick + '.zip'
        # f = zipfile.ZipFile(zipFileName, 'w', zipfile.ZIP_DEFLATED)
        # for excelpath in allExcelPath:
        #     f.write(excelpath)
        # f.close()


        resultJson = {
            # "flag": True,
            # "msg": "succ",
            "sellerNick": sellerNick,
            "getDate": time.strftime("%Y-%m-%d", time.localtime()),
            "day": dayObjArr,  # 1.日表(excel)
            "flow": flowObjArr,  # 2.来源流量(excel)
            "industryRanking": industryranking,  # 3.行业层级排名
            "tradeTrendIndustryCmpr": tradetrendindustrycmprArr,  # 4.交易趋势同业对比(excel)
            "tradeCategory": tradeCategoryArr,  # 5.交易类目构成(excel)
            "deductScore": scoreObj  # 6.店铺扣分
        }

        for excelPath in allExcelPath:
            try:
                os.remove(excelPath)
            except:
                continue
        return True, resultJson  # postSycmResult(resultJson)

        # print '--------Result--------'
        # print '1.日表(excel)_结果:'
        # print dayObjArr
        # print '1.来源流量(excel)_结果:'
        # print flowObjArr
        # print '3.行业层级排名_结果:'
        # print industryranking
        # print '4.交易趋势同行对比(excel)_结果:'
        # print tradetrendindustrycmprArr
        # print '5.交易类目构成(excel)_结果:'
        # print tradeCategoryArr
        # print '6.店铺扣分_结果:'
        # print scoreObj
    except Exception, e:
        traceback.print_exc()
        return False, 'run_sycm Exception: ' + str(e)


def get_date():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


if __name__ == "__main__":
    print 'sycm Start···'
    begin = datetime.datetime.now()
    b, errMsg = run_sycm(sellerNick,
                         'cna=7lG+EtvSmBoCAXDBkfcAb0eT; t=d04cf916dd35e9183288e1f4a9e54bcf; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; tg=0; cookie2=112f3507f7124c1f06ebff1a80e6706c; _tb_token_=e9698ee88f8a5; ali_ab=112.193.145.247.1513653182632.0; UM_distinctid=1606cc29ed8e8-09dee1ac573fb5-5a442916-1fa400-1606cc29ed957e; _m_h5_tk=a0bc5359d77712013b46b2177b9a66fa_1513655529326; _m_h5_tk_enc=18320284691037c734c1d8813353c20e; l=AqOjlAGWoRwc5--sJKU1Cuwhs-xNnzfa; ucn=unsz; _cc_=U%2BGCWk%2F7og%3D%3D; v=0; uc1=cookie14=UoTdeAujYHpqFg%3D%3D&lng=zh_CN&cookie16=VT5L2FSpNgq6fDudInPRgavC%2BQ%3D%3D&existShop=true&cookie21=V32FPkk%2Fhodroid0QSjisQ%3D%3D&tag=8&cookie15=URm48syIIVrSKA%3D%3D&pas=0; uc3=sg2=Aihweedlh43nKu0FPp6Mvq%2FbFhCNO7P%2BivnLyDzlSgw%3D&nk2=Dlkyc5WmuBCdZhM%3D&id2=UNcNO7oUK9bG&vt3=F8dBzLbRznYIcSVCZ5Q%3D&lg2=Vq8l%2BKCLz3%2F65A%3D%3D; existShop=MTUxMzY1NDE2OQ%3D%3D; uss=BxoC1H9OJPhlSMSqKYhxQ98SUeCON1ValL83fBLR5B9dyPvDJV70tHt0; lgc=minghanzhou; tracknick=minghanzhou; sg=u98; mt=np=&ci=0_0; cookie1=VWn70Agy%2F2EvIb6%2B0zfra0MrIoHF6AWbxeBTSWzRSUQ%3D; unb=375014559; skt=aaebd42c734b3559; _l_g_=Ug%3D%3D; _nk_=minghanzhou; cookie17=UNcNO7oUK9bG; isg=AicnClBOMQbCKLWFiip8mH7wtlsxBP9oUZKel_mUQ7bd6EeqAXyL3mXqft4N')
    end = datetime.datetime.now()
    print errMsg if b is False else 'succ'
    print '累计耗时: ' + str(end - begin)  # 不使用代理耗时约1min,代理5-10min
    print 'end.'
