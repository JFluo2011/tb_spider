#!/usr/bin/env python  
# -*- coding:utf-8 -*-

""" 
@version: v1.0 
@author: yvon   
@software: PyCharm 
@file: parseExcel_sycm.py 
@data: 2017/9/6 15:53 
"""

# 说明:
# 解析生意参谋下载的Excel文件

import os, sys, time, xlrd, traceback
from xlrd import XLRDError

reload(sys)
sys.setdefaultencoding('utf-8')

startText_day = '统计日期'
startText_flow = '来源'
startText_tradetrendindustrycpr = '日期'
startText_tradeCategory = '终端'

dict_day = {
    "卖家昵称": "seller_nick",
    "统计日期": "date",
    "被浏览商品数": "viewedproductcount",
    "店铺收藏次数": "shopcollectnum",
    "店铺首页访客数": "homepagevisitorcount",
    "店铺首页浏览量": "homepageviewcount",
    "DSR综合低评分买家数": "dsr_lowscorebuyercount",
    "老访客数": "oldvisitorcount",
    "客单价": "percustomertransaction",
    "服务态度动态评分(DSR)": "taidu_dsr",
    "访客数较前一天变化量": "visitorcountchangethanyesterday",
    "访客数": "visitorcount",
    "老访客数占比": "oldvisitorsproportion",
    "老买家数": "oldbuyercount",
    "老买家数占比": "oldbuyercountproportion",
    "浏览量": "viewcount",
    "浏览量较前一天变化量": "viewcountchangethanyesterday",
    "PC端店铺首页平均停留时长(秒)": "pc_homepagestaydurationavg",
    "PC端店铺首页浏览量": "pc_homepageviewcount",
    "PC端店铺首页访客数": "pc_homepagevisitorcount",
    "PC端被浏览商品数": "pc_viewedproductcount",
    "描述相符动态评分(DSR)": "miaoshu_dsr",
    "PC端访客数": "pc_visitorcount",
    "PC端客单价": "pc_percustomertransaction",
    "PC端老访客数": "pc_oldvisitorcount",
    "PC端浏览量": "pc_viewcount",
    "PC端人均浏览量": "pc_percapitaviewcount",
    "PC端下单金额": "pc_placeorderamount",
    "PC端跳失率": "pc_bouncerate",
    "PC端商品详情页浏览量": "pc_detailpageviewcount",
    "PC端商品详情页访客数": "pc_detailpagevisitorcount",
    "PC端人均停留时长(秒)": "pc_percapitastayduration",
    "PC端下单买家数": "pc_placeorderbuyercount",
    "PC端下单转化率": "pc_placeordertransformationrate",
    "PC端支付父订单数": "pc_paymentparentorderscount",
    "PC端支付金额": "pc_paymentamount",
    "PC端支付老买家数": "pc_paymentoldbuyercount",
    "PC端支付子订单数": "pc_paymentsuborderscount",
    "PC端支付转化率": "pc_paymenttransformationrate",
    "PC端支付商品数": "pc_paymentproductcount",
    "PC端支付商品件数": "pc_paymentproductnum",
    "PC端支付买家数": "pc_panymentbuyercount",
    "人均浏览量（访问深度）": "percapitaviewcount_deep",
    "商品详情页访客数": "detailpagevisitorcount",
    "售中申请退款买家数": "askforrefundbuyercount",
    "手机淘宝APP支付买家数": "phonetbapp_paymentbuyercount",
    "手机淘宝APP支付商品件数": "phonetbapp_paymentproductnum",
    "店铺收藏人数": "shopcollectnumofperson",
    "商品详情页浏览量": "detailpageviewcount",
    "人均停留时长(秒)": "percapitastayduration",
    "人均支付商品件数": "percapitapaymentproductnum",
    "商品详情页支付转化率": "detailpagepaymenttransformationrate",
    "手机淘宝APP访客数": "phonetbapp_visitorcount",
    "淘宝App支付转化率": "tbapp_paymenttransformationrate",
    "天猫App访客数": "tmapp_visitorcount",
    "手机淘宝APP浏览量": "phonetbapp_viewcount",
    "商品收藏人数": "productcollectnumofperson",
    "人均支付子订单数": "percapitapaymentsubordernum",
    "商品收藏次数": "productcollectcount",
    "售中申请退款金额": "askforrefundamount",
    "手机淘宝APP支付金额": "phonetbapp_paymentamount",
    "天猫App浏览量": "tmapp_viewcount",
    "天猫App支付金额": "tmapp_paymentamount",
    "WAP访客数": "wap_visitorcount",
    "Wap支付转化率": "wap_paymenttransformationrate",
    "无线端访客数": "wireless_visitorcount",
    "天猫App支付买家数": "tmapp_paymentbuyercount",
    "WAP浏览量": "wap_viewcount",
    "物流服务动态评分(DSR)": "logisticsservice_dsr",
    "无线端访客数占比": "wireless_visitorproportion",
    "天猫APP支付商品件数": "tmapp_paymentproductnum",
    "WAP支付金额": "wap_paymentamount",
    "无线端被浏览商品数": "wireless_viewedproductcount",
    "无线端客单价": "wireless_percustomertransaction",
    "天猫App支付转化率": "tmapp_paymenttransformationrate",
    "WAP支付买家数": "wap_paymentbuyercount",
    "无线端店铺首页访客数": "wireless_homepagevisitorcount",
    "无线端老访客数": "wireless_oldvisitorcount",
    "跳失率": "bouncerate",
    "WAP支付商品件数": "wap_paymentproductnum",
    "无线端店铺首页浏览量": "wireless_homepageviewcount",
    "无线端浏览量": "wireless_viewcount",
    "无线端人均浏览量": "wireless_percapitaviewcount",
    "无线端人均停留时长(秒)": "wireless_percapitastayduration",
    "无线端商品详情页访客数": "wireless_detailpagevisitorcount",
    "无线端商品详情页浏览量": "wireless_detailpageviewcount",
    "无线端跳失率": "wireless_bouncerate",
    "无线端支付金额": "wireless_paymentamount",
    "无线端支付父订单数": "wireless_paymentparentorderscount",
    "无线端下单转化率": "wireless_placeordertransformationrate",
    "无线端下单买家数": "wireless_placeorderbuyercount",
    "无线端下单金额": "wireless_placeorderamount",
    "无线端支付金额占比": "wireless_paymentamountproportion",
    "无线端支付商品数": "wireless_paymentproductcount",
    "无线端支付老买家数": "wireless_paymentoldbuyercount",
    "无线端支付转化率": "wireless_paymenttransformationrate",
    "无线端支付买家数": "wireless_panymentbuyercount",
    "无线端支付子订单数": "wireless_paymentsuborderscount",
    "无线端支付买家数占比": "wireless_panymentbuyercountproportion",
    "下单父订单数": "placeorderparentorderscount",
    "无线端支付商品件数": "wireless_paymentproductnum",
    "下单金额": "placeorderamount",
    "下单买家数": "placeorderbuyercount",
    "下单商品件数": "placeorderproductnum",
    "已发货父订单数": "deliverdgoodsparentorderscount",
    "支付商品件数": "paymentproductnum",
    "下单且支付父订单数": "placeorderandpaidparentorderscount",
    "下单转化率": "placeordertransformationrate",
    "支付父订单数": "paymentparentorderscount",
    "支付商品数": "paymentproductcount",
    "下单且支付金额": "placeorderandpaidamount",
    "下单子订单数": "placeordersuborderscount",
    "支付金额": "paymentamount",
    "支付转化率": "paymenttransformationrate",
    "下单且支付买家数": "placeorderandpaidbuyercount",
    "新访客数": "newvisitorcount",
    "支付金额较前一天变化量": "paymentchangethanyesterday",
    "支付子订单数": "paymentsuborderscount",
    "下单且支付商品件数": "placeorderandpaidproductnum",
    "新买家数": "newbuyercount",
    "支付买家数": "panymentbuyercount"
}

dict_flowmap = {
    "来源": "source",
    "来源明细": "sourceDetail",
    "访客数": "uv",
    "访客数变化": "UvVariRate",
    "下单买家数": "orderBuyerCnt",
    "下单买家数变化": "OrderBuyerCntVariRate",
    "下单转化率": "OrderRate",
    "下单转化率变化": "OrderVariRate"
}

dict_tradetrendindustrycpr = {
    "日期": "date",
    "终端": "terminalType",
    "支付金额": "payAmt",
    "支付买家数": "payBuyerCnt",
    "客单价": "payPct",
    "下单金额": "orderAmt",
    "下单买家数": "orderBuyerCnt",
    # "下单转化率": "orderRate",  # 已失效
    "支付转化率": "payRate",
    "下单-支付转化率": "prderToPayRate"
}

dict_tradecategory = {
    "终端": "terminalType",
    "一级类目": "firstLevelCategory",
    "二级类目": "secondLevelCategory",
    "叶子类目": "subLevelCategory",
    "支付金额": "payAmt",
    "支付金额占比": "payAmtRate",
    "支付买家数": "payBuyerCnt",
    "支付转化率": "payRate"
}


# 解析日表
def parseDayExcel(filepath, startIdx=3):
    # sycm_d
    dayObjArr_year = []
    dataCount = 0
    try:
        if os.path.exists(filepath) is False:
            return True, None, '解析日Excel失败,excel不存在'
        if os.path.getsize(filepath) == 0:
            return True, None, '解析日Excel失败,size=0'

        wb = xlrd.open_workbook(filepath, encoding_override='utf8')
        sheet = wb.sheet_by_index(0)
        if sheet.row(startIdx)[0].value != startText_day:
            return False, None, '解析日Excel失败,格式非预期'

        # 外层循环行
        rowIdx = startIdx
        for i in xrange(startIdx, sheet.nrows - startIdx):
            rowIdx += 1
            dataCount += 1
            columnIdx = 0
            obj = {"yearmonth": time.strftime("%Y%m", time.strptime(sheet.row(rowIdx)[0].value, "%Y-%m-%d"))}

            # isThisAllEmpty = True
            # 内层循环列
            for j in range(120):
                headerTitle = str(sheet.row(startIdx)[columnIdx].value)
                if not headerTitle:
                    break

                if dict_day.__contains__(headerTitle) is False:
                    columnIdx += 1
                    continue
                key = dict_day[headerTitle]
                value = sheet.row(rowIdx)[columnIdx].value

                # ctype = sheet.row(rowIdx)[columnIdx].ctype
                # if ctype != 1 and ctype != 2:
                #     print str(value) + '-----' + str(sheet.row(rowIdx)[columnIdx].ctype)

                value = "" if value == "-" else str(value).rstrip('%')

                # if value == '-':
                #     value = None
                # else:
                #     isThisAllEmpty = False

                obj[key] = value
                columnIdx += 1
            # print obj
            dayObjArr_year.append(obj)
    except XLRDError, e:
        return True, None, str(e)
    except ValueError, e:
        return False, None, str(e)
    except Exception, e:
        traceback.print_exc()
        errMsg = '解析日表Excel出错,' + str(e)
        return False, None, errMsg

    return True, dayObjArr_year, None


# 解析流量来源_逐月
def parseFlowmapExcel(filepath, date, terminalType=1, startIdx=4):
    # sycm_flowmap
    flowObjArr_month = []
    dataCount = 0
    try:
        if os.path.exists(filepath) is False:
            return True, None, '解析流量来源Excel失败,excel不存在'
        if os.path.getsize(filepath) == 0:
            return True, None, '解析流量来源Excel失败,size=0'

        # wb = xlrd.open_workbook(filepath, encoding_override='utf8')
        wb = xlrd.open_workbook(filepath)
        sheet = wb.sheet_by_index(0)
        if sheet.row(startIdx)[0].value != startText_flow:
            return False, None, '解析流量来源Excel失败,格式非预期'

        # 外层循环行
        rowIdx = startIdx
        for i in xrange(startIdx, sheet.nrows - startIdx):
            rowIdx += 1
            dataCount += 1
            columnIdx = 0

            # type: 0:我的 1:同行平均
            # terminalType: 1:PC 2:无线
            obj = {"month": time.strftime("%Y%m", time.strptime(date, "%Y-%m-%d")), "type": 0,
                   "terminalType": terminalType}

            # isThisAllEmpty = True
            # 内层循环列
            for j in range(len(dict_flowmap)):
                headerTitle = str(sheet.row(startIdx)[columnIdx].value)
                if not headerTitle:
                    break

                if dict_flowmap.__contains__(headerTitle) is False:
                    columnIdx += 1
                    continue
                key = dict_flowmap[headerTitle]
                value = sheet.row(rowIdx)[columnIdx].value

                # ctype = sheet.row(rowIdx)[columnIdx].ctype
                # if ctype != 1 and ctype != 2:
                #     print str(value) + '-----' + str(sheet.row(rowIdx)[columnIdx].ctype)

                value = "" if value == "-" else str(value).rstrip('%')

                # if value == '-':
                #     value = None
                # else:
                #     isThisAllEmpty = False

                obj[key] = str(value.encode('utf8'))
                columnIdx += 1
            # print obj
            flowObjArr_month.append(obj)
    except XLRDError, e:
        return True, None, str(e)
    except ValueError, e:
        return False, None, str(e)
    except Exception, e:
        traceback.print_exc()
        errMsg = '解析流量来源Excel出错,' + str(e)
        return False, None, errMsg

    return True, flowObjArr_month, None


# 解析交易趋势同行对比_逐月
def parseTradetrendindustrycprExcel(filepath, categoryCode, comprType, startIdx=3):
    # sycm_tradetrendindustrycomp
    tradetrendindustrycprObjArr = []
    dataCount = 0

    try:
        if os.path.exists(filepath) is False:
            return True, None, '解析交易趋势同行对比Excel失败,excel不存在'
        if os.path.getsize(filepath) == 0:
            return True, None, '解析交易趋势同行对比Excel失败,size=0'

        # wb = xlrd.open_workbook(filepath, encoding_override='utf8')
        wb = xlrd.open_workbook(filepath)
        sheet = wb.sheet_by_index(0)
        if sheet.row(startIdx)[0].value != startText_tradetrendindustrycpr:
            return False, None, '解析交易趋势同行对比Excel失败,格式非预期'

        # 外层循环行
        rowIdx = startIdx
        for i in xrange(startIdx, sheet.nrows - startIdx):
            rowIdx += 1
            dataCount += 1
            columnIdx = 0

            # 日期以20170908格式存储
            # obj = { "month": time.strftime("%Y%m", time.strptime(sheet.row(rowIdx)[0].value, "%Y-%m-%d")),"categoryCode":categoryCode,"comprType":comprType } # 原日期格式，已失效
            obj = {"month": time.strftime("%Y%m", time.strptime(sheet.row(rowIdx)[0].value, "%Y-%m")),
                   "categoryCode": categoryCode, "comprType": comprType}

            # isThisAllEmpty = True
            # bJumpOver = False
            # 内层循环列
            for j in range(len(dict_tradetrendindustrycpr)):
                headerTitle = str(sheet.row(startIdx)[columnIdx].value)
                if not headerTitle:
                    break

                if dict_tradetrendindustrycpr.__contains__(headerTitle) is False:
                    # bJumpOver = True
                    columnIdx += 1
                    continue
                key = dict_tradetrendindustrycpr[headerTitle]
                value = sheet.row(rowIdx)[columnIdx].value

                # ctype = sheet.row(rowIdx)[columnIdx].ctype
                # if ctype != 1 and ctype != 2:
                #     print str(value) + '-----' + str(sheet.row(rowIdx)[columnIdx].ctype)

                value = "" if value == "-" else str(value).rstrip('%')

                # terminalType: 终端类型(全部为0/PC为1/无线为2)
                if key == "terminalType": value = convertTerminalType(value)

                # if value == '-':
                #     value = None
                # else:
                #     isThisAllEmpty = False

                obj[key] = value
                columnIdx += 1
            # if bJumpOver is False:
            # print obj
            tradetrendindustrycprObjArr.append(obj)
    except XLRDError, e:
        return True, None, str(e)
    except ValueError, e:
        return False, None, str(e)
    except Exception, e:
        traceback.print_exc()
        errMsg = '解析交易趋势同行对比Excel出错,' + str(e)
        return False, None, errMsg

    return True, tradetrendindustrycprObjArr, None


# 解析交易类目构成_逐月
def parseTradeCategoryExcel(filepath, date, startIdx=3):
    # sycm_categorydetail
    tradeCategoryObjArr = []
    dataCount = 0

    try:
        if os.path.exists(filepath) is False:
            return True, None, '解析交易类目构成Excel失败,excel不存在'
        if os.path.getsize(filepath) == 0:
            return True, None, '解析交易类目构成Excel失败,size=0'

        # wb = xlrd.open_workbook(filepath, encoding_override='utf8')
        wb = xlrd.open_workbook(filepath)
        sheet = wb.sheet_by_index(0)
        if sheet.row(startIdx)[0].value != startText_tradeCategory:
            return False, None, '解析交易类目构成Excel失败,格式非预期'

        # 外层循环行
        rowIdx = startIdx
        for i in xrange(startIdx, sheet.nrows - startIdx):
            rowIdx += 1
            dataCount += 1
            columnIdx = 0

            # 日期以20170908格式存储
            obj = {"month": time.strftime("%Y%m", time.strptime(date, "%Y-%m-%d"))}

            # isThisAllEmpty = True
            # bJumpOver = False
            # 内层循环列
            for j in range(len(dict_tradecategory)):
                headerTitle = str(sheet.row(startIdx)[columnIdx].value)
                if not headerTitle:
                    break

                if dict_tradecategory.__contains__(headerTitle) is False:
                    # bJumpOver = True
                    columnIdx += 1
                    continue
                key = dict_tradecategory[headerTitle]
                value = sheet.row(rowIdx)[columnIdx].value

                # ctype = sheet.row(rowIdx)[columnIdx].ctype
                # if ctype != 1 and ctype != 2:
                #     print str(value) + '-----' + str(sheet.row(rowIdx)[columnIdx].ctype)

                value = "" if value == "-" else str(value).rstrip('%')

                # terminalType: 终端类型(全部为0/PC为1/无线为2)
                if key == "terminalType": value = convertTerminalType(value)

                # if value == '-':
                #     value = None
                # else:
                #     isThisAllEmpty = False

                obj[key] = value
                columnIdx += 1
            # if bJumpOver is False:
            # print obj
            tradeCategoryObjArr.append(obj)
    except XLRDError, e:
        return True, None, str(e)
    except ValueError, e:
        return False, None, str(e)
    except Exception, e:
        traceback.print_exc()
        errMsg = '解析交易类目构成Excel出错,' + str(e)
        return False, None, errMsg

    return True, tradeCategoryObjArr, None


# terminalType: 终端类型(全部为0/PC为1/无线为2)
def convertTerminalType(value):
    if value == '所有终端':
        return 0
    elif value == 'pc端':
        return 1
    elif value == '无线端':
        return 2
    else:
        return value
