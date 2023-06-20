import pandas as pd
import importlib

from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict

app = FastAPI()
baostock_lib = importlib.import_module('baostock')
akshare_lib = importlib.import_module('akshare')


class ProxyBody(BaseModel):
    method: str
    args: Union[Dict, None] = None


@app.post("/baostock/")
async def baostock_proxy(body: ProxyBody):
    try:
        lg = getattr(baostock_lib, 'login')()
        if lg is None:
            return {"code": -1, "msg": '登录失败'}
        if lg.error_code != '0':
            return {"code": lg.error_code, "msg": lg.error_msg}
        rs_list = []
        method = getattr(baostock_lib, body.method)
        result = method(**body.args)
        if result is None:
            return {"code": -1, "msg": '参数异常'}

        if result.error_code != '0':
            return {"code": result.error_code, "msg": result.error_msg}

        while result.next():
            rs_list.append(result.get_row_data())
        getattr(baostock_lib, 'logout')()

        df = pd.DataFrame(rs_list, columns=result.fields)
        data = df.fillna(0).to_dict('records')
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}


@app.post("/akshare/")
async def akshare_proxy(body: ProxyBody):
    try:
        method = getattr(akshare_lib, body.method)
        if body.args is None:
            result = method()
        else:
            result = method(**body.args)

        data = None
        if result is not None:
            data = result.fillna(0).to_dict('records')
        return {"code": 0, "data": data}
    except Exception as e:
        return {"code": -1, "msg": str(e)}
