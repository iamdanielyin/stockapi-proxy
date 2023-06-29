import os
import importlib
import concurrent.futures
import datetime
import pandas as pd
import tushare as ts
import baostock as bs
import akshare as ak

from typing import Union
from fastapi import FastAPI, Path
from pydantic import BaseModel
from typing import Dict

app = FastAPI()
app.state.tushare_pro = None
baostock_lib = importlib.import_module('baostock')
akshare_lib = importlib.import_module('akshare')
tushare_lib = importlib.import_module('tushare')


@app.post("/baostock/{method}")
async def baostock_proxy(method: str = Path(title="Baostock API"), args: Union[Dict, None] = None):
    try:
        lg = getattr(baostock_lib, 'login')()
        if lg is None:
            return {"code": -1, "msg": '登录失败'}
        if lg.error_code != '0':
            return {"code": int(lg.error_code), "msg": lg.error_msg}
        rs_list = []
        method = getattr(baostock_lib, method)
        result = method(**args)
        if result is None:
            return {"code": -1, "msg": '参数异常'}

        if result.error_code != '0':
            return {"code": int(result.error_code), "msg": result.error_msg}

        while result.next():
            rs_list.append(result.get_row_data())
        getattr(baostock_lib, 'logout')()

        df = pd.DataFrame(rs_list, columns=result.fields)
        data = df.fillna(0).to_dict('records')
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.post("/akshare/{method}")
async def akshare_proxy(method: str = Path(title="Akshare API"), args: Union[Dict, None] = None):
    try:
        method = getattr(akshare_lib, method)
        if args is None:
            result = method()
        else:
            result = method(**args)

        data = None
        if result is not None:
            data = result.fillna(0).to_dict('records')
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.post("/tushare/{method}")
async def tushare_proxy(method: str = Path(title="Tushare API"), args: Union[Dict, None] = None):
    try:
        if app.state.tushare_pro is None:
            token = os.getenv('TUSHARE_TOKEN')
            app.state.tushare_pro = ts.pro_api(token)
        method = getattr(app.state.tushare_pro, method)
        if args is None:
            result = method()
        else:
            result = method(**args)
        data = None
        if result is not None:
            data = result.fillna(0).to_dict('records')
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.get("/trade_date")
async def trade_date():
    try:
        data = ak.tool_trade_date_hist_sina().loc[:, "trade_date"].values.tolist()
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.get("/spot")
async def spot():
    try:
        data = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            def call_api(market):
                match market:
                    case 'sh':
                        df = ak.stock_sh_a_spot_em().fillna(0)
                        df['交易所'] = ['上证交易所'] * len(df.index)
                        data.extend(df.to_dict('records'))
                    case 'sz':
                        df = ak.stock_sz_a_spot_em().fillna(0)
                        df['交易所'] = ['深证交易所'] * len(df.index)
                        data.extend(df.to_dict('records'))
                    case 'bj':
                        df = ak.stock_bj_a_spot_em().fillna(0)
                        df['交易所'] = ['北证交易所'] * len(df.index)
                        data.extend(df.to_dict('records'))

            executor.map(call_api, ['sh', 'sz', 'bj'])
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.get("/stocks_simple")
async def stocks_simple():
    try:
        data = ak.stock_info_a_code_name().to_dict('records')
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.get("/stocks_full")
async def stocks_full():
    try:
        data = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            def call_api(market):
                match market:
                    case 'sh':
                        df = ak.stock_info_sh_name_code(symbol="主板A股").fillna(0)
                        df['交易所'] = ['上证交易所'] * len(df.index)
                        data.extend(df.rename(columns={
                            '公司代码': '证券代码',
                            '公司简称': '证券简称'
                        }).to_dict('records'))
                    case 'sz':
                        df = ak.stock_info_sz_name_code(symbol="A股列表").rename(columns={
                            'A股代码': '证券代码',
                            'A股简称': '证券简称',
                            'A股上市日期': '上市日期',
                            'A股总股本': '总股本',
                            'A股流通股本': '流通股本',
                        })
                        df['交易所'] = ['深证交易所'] * len(df.index)
                        df['总股本'] = pd.to_numeric(df['总股本'].str.replace(',', ''), errors='coerce')
                        df['流通股本'] = pd.to_numeric(df['流通股本'].str.replace(',', ''), errors='coerce')
                        df = df.fillna(0)
                        data.extend(df.to_dict('records'))
                    case 'bj':
                        df = ak.stock_info_bj_name_code().fillna(0)
                        df['交易所'] = ['北证交易所'] * len(df.index)
                        data.extend(df.to_dict('records'))

            executor.map(call_api, ['sh', 'sz', 'bj'])
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.get("/dividend")
async def stock_dividend(code: str, year: str = datetime.datetime.now().year, year_type: str = 'report'):
    return await baostock_proxy('query_dividend_data', {
        'code': code,
        'year': year,
        'yearType': year_type
    })


@app.get("/kdata")
async def kdata(code: str, freq: str, start: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                end: Union[str, None] = None, adjust: str = '2', fields: Union[str, None] = None):
    try:
        if fields is None or fields == '':
            match freq:
                case 'd':
                    fields = 'date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST'
                case 'w' | 'm':
                    fields = 'date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg'
                case _:
                    fields = 'date,time,code,open,high,low,close,volume,amount,adjustflag'

        if end is None or end == '':
            end = start

        return await baostock_proxy('query_history_k_data_plus', {
            'code': code,
            'fields': fields,
            'start_date': start,
            'end_date': end,
            'frequency': freq,
            'adjustflag': adjust,
        })
    except Exception as e:
        return {"code": -1, "msg": str(e)}
