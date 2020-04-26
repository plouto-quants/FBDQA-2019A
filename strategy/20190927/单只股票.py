'''
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
'''

'''
股票：一只股票

买入信号：
    MA5上穿MA30
卖出信号：
    MA5下穿MA30

交易的时间级别：日线
'''
# 导入函数库
import jqdata
import pandas as pd
import numpy as np
import math
import talib as tl

# 买入信号中的短时均线长度
PL_BUY_SHORT_MA  = 5
# 买入信号中的长时均线长度
PL_BUY_LONG_MA   = 30
# 卖出信号中的短时均线长度
PL_SELL_SHORT_MA = 5
# 卖出信号中的长时均线长度
PL_SELL_LONG_MA  = 30

# 初始化函数，设定基准等等
def initialize(context):
    # 设定贵州茅台作为基准
    set_benchmark('600519.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    #log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 设定滑点为0
    set_slippage(FixedSlippage(0))
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    pl_init_global(context)
    # 开盘前运行
    run_daily(pl_before_market_open, time='before_open', reference_security='000300.XSHG')
    # 交易
    run_daily(pl_trade, time='every_bar',reference_security='000300.XSHG')
    # 收盘后运行
    run_daily(pl_after_market_close, time='after_close', reference_security='000300.XSHG')


# 初始化全局变量
def pl_init_global(context):
    # 股票池，贵州茅台
    g.pl_stock_pool = ['600519.XSHG']


## 开盘前运行函数
def pl_before_market_open(context):
    pass


def pl_trade(context):
    pl_sell(context)
    pl_buy(context)   # 建仓

## 收盘后运行函数
def pl_after_market_close(context):
    # 绘制当天的仓位
    pl_cash = context.portfolio.available_cash
    pl_total = context.portfolio.total_value
    pl_position = (pl_total - pl_cash)/pl_total * 100
    record(position=pl_position)
    return

def pl_buy(context):
    '''
    买入逻辑
    '''
    pl_current_datas = get_current_data()
    for pl_code in g.pl_stock_pool:
        pl_current_data = pl_current_datas[pl_code]
        if pl_current_data == None:
            continue
        if pl_code in context.portfolio.positions.keys():
            # 已经有持仓，不再判断建仓信号
            continue
        if pl_is_high_limit(pl_code):
            continue

        # 计算需要的数量个数，因为判断上穿最多需要用到3个数据，所以需要多加载两个收盘价
        pl_count = max(PL_BUY_SHORT_MA,PL_BUY_LONG_MA) + 2
        pl_close_data = attribute_history(security=pl_code, count=pl_count, unit='1d',fields=['close'],skip_paused=True, df=True, fq='pre')['close']
        if (list(np.isnan(pl_close_data)).count(True) > 0) or (len(list(pl_close_data)) < pl_count):
            continue

        #上穿
        pl_short_ma = pd.rolling_mean(pl_close_data,PL_BUY_SHORT_MA)
        pl_long_ma  = pd.rolling_mean(pl_close_data,PL_BUY_LONG_MA)

        if pl_cross(pl_short_ma,pl_long_ma) > 0:
            # 计算头寸
            pl_position_value = pl_calc_position(context,pl_code)
            # 买入股票
            pl_order_ = order_value(security=pl_code, value=pl_position_value)
            if (pl_order_ is not None) and (pl_order_.filled > 0):
                log.info("交易 买入",pl_code,"预期买入",pl_order_.amount,"实际买入",pl_order_.filled)
    return


def pl_sell(context):
    '''
    卖出逻辑
    '''
    pl_current_datas = get_current_data()
    for pl_code in context.portfolio.positions.keys():
        pl_current_data = pl_current_datas[pl_code]
        if pl_current_data == None:
            continue
        if pl_is_low_limit(pl_code):
            continue
        pl_position = context.portfolio.positions[pl_code]
        if pl_position.closeable_amount <= 0:
            continue

        # 计算需要的数量个数，因为判断下穿最多需要用到3个数据，所以需要多加载两个收盘价
        pl_count = max(PL_SELL_SHORT_MA,PL_SELL_LONG_MA) + 2
        pl_close_data = attribute_history(security=pl_code, count=pl_count, unit='1d',fields=['close'],skip_paused=True, df=True, fq='pre')['close']

        if (list(np.isnan(pl_close_data)).count(True) > 0) or (len(list(pl_close_data)) < pl_count):
            continue

        #下穿
        pl_short_ma = pd.rolling_mean(pl_close_data,PL_SELL_SHORT_MA)
        pl_long_ma  = pd.rolling_mean(pl_close_data,PL_SELL_LONG_MA)

        if pl_cross(pl_short_ma,pl_long_ma) < 0:
            # 卖出股票
            pl_order_ = order_target(security=pl_code, amount=0)
            if (pl_order_ is not None) and (pl_order_.filled > 0):
                log.info("交易 卖出",pl_code,"卖出数量",pl_order_.filled,"剩余数量",(pl_order_.amount - pl_order_.filled))
    return


def pl_calc_position(context,pl_code):
    '''
    计算仓位。

    全仓买入
    '''
    return context.portfolio.available_cash


def pl_is_high_limit(pl_code):
    '''
    判断标的是否已经涨停或停牌

    Args:
        pl_code 标的的代码。如要检测平安银行需要传入参数 000001.XSHE
    Returns:
        True 表示要检测的标的涨停或停牌，这时不能进行买入操作
        False 表示要检测的标的没有涨停或停牌，可以进行买入操作
    '''
    pl_current_data = get_current_data()[pl_code]
    if pl_current_data.last_price >= pl_current_data.high_limit:
        return True
    if pl_current_data.paused:
        return True
    return False



def pl_is_low_limit(pl_code):
    '''
    判断标的是否已经跌停或停牌

    Args:
        pl_code 标的的代码。
    Returns:
        True 表示要检测的标的涨停或停牌，这时不能进行卖出操作
        False 表示要检测的标的没有涨停或停牌，可以进行卖出操作
    '''
    pl_current_data = get_current_data()[pl_code]
    if pl_current_data.last_price <= pl_current_data.low_limit:
        return True
    if pl_current_data.paused:
        return True
    return False


def pl_cross(pl_series1, pl_series2):
    '''
    判断 pl_series1 和 pl_series2 的交叉情况

    Args:
        pl_series1 系列1 最少包含3个数据
        pl_series2 系列2 最少包含3个数据
    Returns:
        1  pl_series1 上穿 pl_series2
        0  pl_series1 没有和 pl_series2 发生交叉
        -1 pl_series1 下穿 pl_series2
    '''
    if pl_series1[-1] > pl_series2[-1]:
        if pl_series1[-2] < pl_series2[-2]:
            return 1
        elif pl_series1[-2] == pl_series2[-2]:
            if pl_series1[-3] < pl_series2[-3]:
                return 1

    elif pl_series1[-1] < pl_series2[-1]:
        if pl_series1[-2] > pl_series2[-2]:
            return -1
        elif pl_series1[-2] == pl_series2[-2]:
            if pl_series1[-3] > pl_series2[-3]:
                return -1
    return 0

