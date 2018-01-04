#!/usr/bin/env python  
# -*- coding:utf-8 -*-

""" 
@version: v1.0 
@author: yvon   
@software: PyCharm 
@file: tbLoan.py 
@data: 2017/9/13 15:53 
"""

# 说明:
# 从网商银行抓取淘宝贷款(https://loan.mybank.cn),未使用代理IP
# 执行顺序: 淘宝订单及其他 -> 淘宝贷款 -> 生意参谋.
# 完成后通过http调用接口回传结果.需要抓取3部分指标:
# 1.记录查询(最近1年)
# 2.还款总览
# 3.首页4款贷款产品

import sys, traceback, requests, datetime, time, urlparse, json
import common

reload(sys)
sys.setdefaultencoding('utf-8')
requests.packages.urllib3.disable_warnings()

# 淘宝贷款结果回调url
cbUrl = 'http://192.168.1.40:8080/stroeUploadData/uploadTaobaoLoan'
bLoginRequired = False
loanCookies = ''  # 淘宝贷款cookie(https://loan.mybank.cn/loan/loan.htm?c=taobao&c_ext=tbalipay)
ctoken = ''  # loanCookies中提取
sellerNick = ''  # sellerNick
repaySummary = {}  # 还款总览_结果
recordList = []  # 记录查询(最近1年)_结果
loanProduct = {}  # 首页4款贷款产品_结果


# 还款记录(未启用)
# def getRepayPlanList():
#     try:
#         global recordListAll, loanCookies, ctoken
#         url = 'https://loan.mybank.cn/loan/repayPlanList.json?c=taobao&_input_charset=utf-8&_callback=cbk&ctoken=' + ctoken
#         b, html, errMsg = makeRequest(url)
#         if b is False:
#             common.printWarning('getRepayPlanList fail: ' + errMsg)
#             return False, errMsg
#
#         # /**/cbk({"data":{"repayPlan":{"systemDate":1505318400000,"repayPlanList":[{"prinAmt":83.33,"fineAmt":0.00,"repayAccount":["min***@gmail.com"],"repayAccountType":"ALIPAY","bizNo":"226610000045906258_2016100101042002058477654257_20171003_2088302120950905_01021000100000000244","repayDate":1506960000000,"chargeAmt":0.00,"repayAmt":85.50,"intAmt":2.17},{"prinAmt":83.37,"fineAmt":0.00,"repayAccount":["min***@gmail.com"],"repayAccountType":"ALIPAY","bizNo":"226610000045906258_2016100101042002058477654257_20171103_2088302120950905_01021000100000000244","repayDate":1509638400000,"chargeAmt":0.00,"repayAmt":84.49,"intAmt":1.12}],"ovdInfo":{"ovdAmt":"0.00","prinAmt":"0.00","fineAmt":"0.00","repayAccount":[],"allFineDisplayAmt":"0.00","repayAccountType":"Alipay","bizNo":[],"chargeAmt":"0.00","intAmt":"0.00"},"isOvd":false}},"reqInfo":{"resultMsg":"","traceId":"0a378cd015053697974574929","resultCode":"","showType":"0","success":"true"}})
#         jsonObj = json.loads(html[8:len(html) - 1])
#         if jsonObj is None or jsonObj.has_key('data') is False:
#             errMsg = 'result json is wrong'
#             common.printWarning('getRepayPlanList fail: ' + errMsg)
#             return False, errMsg
#
#         if jsonObj['data'].has_key('applyList') is False or len(jsonObj['data']['applyList']) == 0:
#             return True, 'not loan yet.'  # 不曾贷款过
#
#         recordListAll.append(jsonObj['data']['applyList'])
#         if jsonObj['data']['hasNext'] is False:
#             # Todo:只有1页
#             pass
#
#         return False, None
#     except Exception, e:
#         traceback.print_exc()
#         errMsg = str(e)
#         common.printWarning('getRepayPlanList exception: ' + errMsg)
#         return False, errMsg


# 记录查询(最近1年)
def getLoanRecord():
    try:
        global ctoken, recordList
        pageIdx = 1

        url = 'https://loan.mybank.cn/loan/list/credit_list.json?applyTime=D356&loanTerm=ALL&repayType=RA&loanStatus=ALL&startDate=1307801600000&endDate=2005404799000&pageNumber=' + str(
            pageIdx) + '&_input_charset=utf-8&_callback=cbk&ctoken=' + ctoken
        b, html, errMsg = makeRequest(url)
        if b is False:
            common.printWarning('getLoanRecord fail: ' + errMsg)
            return False, errMsg

        # /**/cbk({"loanTradeListHistoryRequestDo":{"startDate":"1397801600000","loanStatus":"ALL","applyTime":"D356","pageNumber":"1","endDate":"1605404799000","repayType":"RA","loanTerm":"ALL"},"data":{"startDate":1497852179126,"applyList":[{"lendDetailUrl":"https%3A%2F%2Flogin.mybank.cn%2Faso%2Ftgs%3Ftarget%3Dhttps%253A%252F%252Floan.mybank.cn%252Ftradmgr%252Ftrade%252FtradeDetailRouter.htm%253FserviceKey%253DMYBANK%2526applySeqno%253D20252016110387178842B","status":"NORMAL","loanDt":"2016.11.03","loanArNo":"20252016110300663779H","loanAmt":"1000.00","repayMode":"2","loanTermCode":"12M","repayModeDesc":"等额本金","bizNo":"20252016110387178842B","channel":"淘宝","loanTerm":"12个月","statusDesc":"使用中"}],"hasNext":false,"nowPage":1,"endDate":1505368979126},"reqInfo":{"resultMsg":"","traceId":"0a378c3515053689791221464","resultCode":"","showType":"0","success":"true"}})
        jsonObj = json.loads(html[8:len(html) - 1])
        if jsonObj is None or jsonObj.has_key('data') is False:
            errMsg = 'result json is wrong'
            common.printWarning('getLoanRecord fail: ' + errMsg)
            return False, errMsg

        if jsonObj['data'].has_key('applyList') is False or len(jsonObj['data']['applyList']) == 0:
            return True, 'not loan yet.'  # 不曾贷款过

        recordTmp = jsonObj['data']['applyList']
        for r in recordTmp:
            r['loanDt'] = r['loanDt'].replace('.', '-')
        recordList = recordTmp
        if jsonObj['data']['hasNext'] is False:
            # Todo:只有1页
            pass

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getLoanRecord exception: ' + errMsg)
        return False, errMsg


# 还款总览
def getRepaySummary():
    try:
        global ctoken, repaySummary
        url = 'https://loan.mybank.cn/loantrade/repay/plan/summary.json?_input_charset=utf-8&_callback=cbk&ctoken=' + ctoken
        b, html, errMsg = makeRequest(url)
        if b is False:
            common.printWarning('getRepaySummary fail: ' + errMsg)
            return False, errMsg

        # /**/cbk({"data":{"btnEnable":"true","orderOvdPrinAmt":"0","orderPrinAmt":"0","repayAccounts":[{"accNo":"min***@gmail.com","accNoType":"ALIPAY","accSign":"minghanzhou@gmail.com"}],"totalOvdPrinAmt":"0","totalPrinAmt":"166.70"},"reqInfo":{"showType":"0","success":"true"}})
        jsonObj = json.loads(html[8:len(html) - 1])
        if jsonObj is None or jsonObj.has_key('data') is False:
            errMsg = 'result json is wrong'
            common.printWarning('getRepaySummary fail: ' + errMsg)
            return False, errMsg

        repaySummary = {
            'orderOvdPrinAmt': jsonObj['data']['orderOvdPrinAmt'],  #
            'orderPrinAmt': jsonObj['data']['orderPrinAmt'],  # 订单未还金额 ?
            'totalOvdPrinAmt': jsonObj['data']['totalOvdPrinAmt'],  #
            'totalPrinAmt': jsonObj['data']['totalPrinAmt']  # 未还本金（元）
        }

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getRepaySummary exception: ' + errMsg)
        return False, errMsg


# 首页4款贷款产品
def getLoanProduct():
    try:
        global ctoken, loanProduct
        url = 'https://loan.mybank.cn/loan/indexList.json?c=taobao&_input_charset=utf-8&_callback=cbk&ctoken=' + ctoken
        b, html, errMsg = makeRequest(url)
        if b is False:
            common.printWarning('getLoanProduct fail: ' + errMsg)
            return False, errMsg

        # /**/cbk({"data":{"defaultSelectedProduct":"LN12022","prodInfo":[{"loanPolicyCode":"BC12003","custSegFlag":"TAOBAO_TBSP","creditStartDate":"1505318400000","loanTermUnit":"N","dailyIntRatePricingType":"%","creditExpireDate":"1520956800000","loanTitle":"等额本金(12个月)","intRatePricing":"0.144","dailyIntRatePricing":"0.0400","dailyBaseRate":"0.065","loanProdCode":"LN12022","repayMode":"2","originLoanableAmt":"22833.00","loanTerm":"12个月","loanableAmount":"22833.00","productCode":"01021000100000000206","switchFlag":"true","loanTermDesc":"12个月","custSegRelations":[],"creditExtData":{},"creditLmtAmount":"23000.00","ableAmount":"22833.00","custOrg":"226610000004558864841","creditSource":"PRE","repayModeName":"等额本金"},{"loanTitleTip":"暂不符合申请条件","loanTermUnit":"N","dailyIntRatePricingType":"%","loanTitle":"随借随还(6个月)","dailyIntRatePricing":"0.05","dailyBaseRate":"0.065","loanTerm":"6个月","productCode":"LN22002,LN12002,LN22003,LN12029,LN12030,LN22022","switchFlag":"true","loanTermDesc":"6个月","custSegRelations":[],"repayModeName":"随借随还"},{"loanTitleTip":"暂不符合申请条件","loanTermUnit":"N","dailyIntRatePricingType":"%","loanTitle":"组合贷款","dailyIntRatePricing":"0.05","dailyBaseRate":"0.065","loanTerm":"12个月","productCode":"LN12012,LN12015,LN22008,LN22009,LN12023,LN12025,LN12024,LN12026,LN22023,LN22024","switchFlag":"true","loanTermDesc":"12个月","custSegRelations":[],"repayModeName":"组合贷款"},{"loanTitleTip":"暂不符合申请条件","loanTermUnit":"N","dailyIntRatePricingType":"%","loanTitle":"订单贷款","dailyIntRatePricing":"0.0483","dailyBaseRate":"","loanTerm":"最长60天","productCode":"LN11002,LN11003,LN21002","switchFlag":"true","loanTermDesc":"最长60天","custSegRelations":[],"repayModeName":"自动还款"}],"loanApplyInfos":[]},"reqInfo":{"resultMsg":"","traceId":"0a37812515053772831552714","resultCode":"","showType":"0","success":"true"}})
        jsonObj = json.loads(html[8:len(html) - 1])
        if jsonObj is None or jsonObj.has_key('data') is False or jsonObj['data'].has_key('prodInfo') is False:
            errMsg = 'result json is wrong'
            common.printWarning('loanProduct fail: ' + errMsg)
            return False, errMsg

        for item in jsonObj['data']['prodInfo']:
            loanCate = item['loanTitle']
            if loanCate == '等额本金(12个月)':
                loanProduct['debj_12'] = item
            elif loanCate == '随借随还(6个月)':
                loanProduct['sjsh_6'] = item
            elif loanCate == '组合贷款':
                loanProduct['zhdk'] = item
            elif loanCate == '订单贷款':
                loanProduct['dddk'] = item

        return True, None
    except Exception, e:
        traceback.print_exc()
        errMsg = str(e)
        common.printWarning('getLoanProduct exception: ' + errMsg)
        return False, errMsg


# 生成header
def getLoanheader(url):
    global loanCookies
    sycm_hearder = {
        'Host': urlparse.urlparse(url).netloc,
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cookie': loanCookies
    }
    return sycm_hearder


# 生成请求
def makeRequest(url):
    for i in range(3):
        errMsg = ''
        try:
            r = requests.get(url, headers=getLoanheader(url), allow_redirects=False, verify=False, timeout=15)
            # r = requests.get(url, headers=getLoanheader(url), allow_redirects=False, verify=False,
            #                  proxies=common.getProxy(), timeout=25)
            if r.status_code != 200:
                if r.status_code == 302 and r.headers['Location'].__contains__('login'):
                    global bLoginRequired
                    bLoginRequired = True
                    errMsg = 'You must login system first.'
                else:
                    errMsg = str(r.status_code)
                continue
            elif r.text.__contains__('deny'):
                errMsg = 'LoginRequired'
                return False, None, errMsg

            html = r.text
            return True, html, errMsg
        except Exception, e:
            traceback.print_exc()
            errMsg = str(e)
            continue
    return False, None, errMsg


def postLoanResult(resultJson):
    global cbUrl
    errMsg = 'postLoanResult fail'
    for i in range(3):
        try:
            r = requests.post(cbUrl, data=json.dumps(resultJson),
                              headers={'Content-Type': 'application/json;charset=UTF-8'}, timeout=20)
            # {"obj":null,"status":"0","code":null,"message":"处理成功","totalCount":0}
            rspJson = json.loads(r.text)
            if r.status_code != 200:
                return False, 'postLoanResult fail: ' + str(r.status_code)
            elif rspJson['status'] != "0":
                return False, 'postLoanResult fail: ' + rspJson['message']

            return True, None
        except Exception, e:
            traceback.print_exc()
            errMsg = 'postLoanResult Exception: ' + str(e)
            continue
    return False, errMsg


# 执行淘宝贷款抓取
def run_tbLoan(sellernick_, loancookies_):
    try:
        global loanCookies, ctoken, sellerNick, repaySummary, recordList, loanProduct
        ctoken = ''
        loanCookies = loancookies_  # 淘宝贷款cookies
        sellerNick = sellernick_  # sellerNick

        for c in loanCookies.split(';'):
            if c.strip().startswith('ctoken'):
                ctoken = c.split('=')[1]
                break

        if ctoken == '':
            return False, 'loanCookies invalid,no ctoken found.'

        repaySummary = {}
        recordList = []
        loanProduct = {}
        resultJsonFail = {
            "flag": False,  # 状态标识
            "msg": "fail",  # 错误信息
            "getDate": time.strftime("%Y-%m-%d", time.localtime()),  # 抓取时间
            "sellerNick": sellerNick,  # sellerNick
            'repaySummary': {},  # 还款总览
            'prinAmt': {},  # 未还金额
            'recordList': [],  # 记录查询(最近1年)
            'loanProduct': {}  # 首页4款贷款产品
        }
        b, errMsg = getLoanProduct()  # 首页各个指标
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postLoanResult(resultJsonFail)
            return False, errMsg
        b, errMsg = getLoanRecord()  # 记录查询(最近1年)
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postLoanResult(resultJsonFail)
            return False, errMsg
        b, errMsg = getRepaySummary()  # 还款总览
        if b is False:
            resultJsonFail['msg'] = errMsg
            # postLoanResult(resultJsonFail)
            return False, errMsg

        resultJson = {
            # "flag": True,  # 状态标识
            # "msg": "succ",  # 错误信息
            "getDate": time.strftime("%Y-%m-%d", time.localtime()),  # 抓取时间
            "sellerNick": sellerNick,  # sellerNick
            'repaySummary': repaySummary,  # 还款总览
            'recordList': recordList,  # 记录查询(最近1年)
            'loanProduct': loanProduct  # 首页4款贷款产品
        }

        print resultJson
        return True, resultJson  # postLoanResult(resultJson)
    except Exception, e:
        traceback.print_exc()
        return False, 'run_tbLoan Exception: ' + str(e)


if __name__ == '__main__':
    print 'loan Start···'
    begin = datetime.datetime.now()
    b, errMsg = run_tbLoan('sellerNick',
                           'JSESSIONID=LfKIiraeaOFoPBlKvNyzYwwpeAfcauthcenter; cna=2rGVEk70fFcCAWXM2pv1/sN2; session.cookieNameId=""; ctoken=0jBhpfhssZ6sAmY4; ALIPAYJSESSIONID=LfKIiraeaOFoPBlKvNyzYwwpeAfcauthcenter; MYBANKJSESSIONID=LfKIiraeaOFoPBlKvNyzYwwpeAfcauthcenter; LoginForm=TAOBAO; ali_apache_tracktmp="uid=2088302120950905"; apay_trk_src="sid=375014559"; isg=As_PElPiydjdLc2hUDJzRUiuXmMZXCHSJTA5XeHcGz5FsO2y6cekZ9ToxNb1; c=taobao; JSESSIONID=0245E08098E76B16BE627BE91B1B9F44; spanner=71RVZ2Ng4ZJWW0S+YIsOSvkqiddMeUT/Xt2T4qEYgj0=')
    end = datetime.datetime.now()
    print errMsg if b is False else 'succ'
    print '累计耗时: ' + str(end - begin)
    print 'end.'
