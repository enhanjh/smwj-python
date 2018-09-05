# python 3.6 32bit
# installed package
# 1. win32com(pywin32)

import win32com.client
import pythoncom
import db_config as dbconf

# 1. login
class XASessionEventHandler:
    login_state = 0

    def OnLogin(self, code, msg):
        if code == "0000":
            print("로그인 성공")
            XASessionEventHandler.login_state = 1
        else:
            print("로그인 실패")


instXASession = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEventHandler)

id = **dbconf.api_cred.id
passwd = **dbconf.api_cred.idpw
cert_passwd = **dbconf.api_cred.certpw

instXASession.ConnectServer("hts.ebestsec.co.kr", 20001)
instXASession.Login(id, passwd, cert_passwd, 0, 0)

while XASessionEventHandler.login_state == 0:
    pythoncom.PumpWaitingMessages()

num_account = instXASession.GetAccountListCount()
for i in range(num_account):
    account = instXASession.GetAccountList(i)
    print(account)


# 2. retrieve
class XAQueryEventHandlerT8413:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT8413.query_state = 1

instXAQueryT8413 = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEventHandlerT8413)
instXAQueryT8413.ResFileName = "C:\\eBEST\\xingAPI\\Res\\T8413.res"

instXAQueryT8413.SetFieldData("t8413InBlock", "shcode", 0, "122630")
instXAQueryT8413.SetFieldData("t8413InBlock", "gubun", 0, "2")
instXAQueryT8413.SetFieldData("t8413InBlock", "sdate", 0, "19960101")
instXAQueryT8413.SetFieldData("t8413InBlock", "edate", 0, "20180904")
instXAQueryT8413.SetFieldData("t8413InBlock", "comp_yn", 0, "Y")

instXAQueryT8413.Request(0)

while XAQueryEventHandlerT8413.query_state == 0:
    pythoncom.PumpWaitingMessages()

count = instXAQueryT8413.GetBlockCount("t8413OutBlock1")
for i in range(count):
    date = instXAQueryT8413.GetFieldData("t8413OutBlock1", "date", i)
    open = instXAQueryT8413.GetFieldData("t8413OutBlock1", "open", i)
    high = instXAQueryT8413.GetFieldData("t8413OutBlock1", "high", i)
    low = instXAQueryT8413.GetFieldData("t8413OutBlock1", "low", i)
    close = instXAQueryT8413.GetFieldData("t8413OutBlock1", "close", i)
    print(date, open, high, low, close)