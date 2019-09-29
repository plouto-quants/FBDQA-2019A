'''
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
'''

'''
股票池：
    1. 剔除ST的股票
    2. 剔除总市值排名最小的10%的股票
    3. 剔除PE TTM 小于0或大于100的数据
    4. 取25日跌幅前10%的股票

    调整周期：25个交易日

买入信号：
    MA3上穿MA200
卖出信号：
    MA3下穿MA200

交易的时间级别：20分钟线
'''
# 导入函数库
import jqdata
import pandas as pd
import numpy as np
import math
import talib as tl

# 股票池计算涨跌幅的窗口大小
PL_CHANGE_PCT_DAY_NUMBER = 25
# 更新股票池的间隔天数
PL_CHANGE_STOCK_POOL_DAY_NUMBER = 25
# 两次处理交易逻辑的窗口大小
PL_TRADE_BAR_DURATION = 20
# 计算数据时的bar的单位
PL_UNIT = str(PL_TRADE_BAR_DURATION) + 'm'
# 买入信号中的短时均线长度
PL_BUY_SHORT_MA  = 3
# 买入信号中的长时均线长度
PL_BUY_LONG_MA   = 100
# 卖出信号中的短时均线长度
PL_SELL_SHORT_MA = 3
# 卖出信号中的长时均线长度
PL_SELL_LONG_MA  = 100

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
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
    # 距上一次股票池更新的天数
    g.pl_stock_pool_update_day = 0
    # 股票池，股票代码
    g.pl_stock_pool = []
    # 距离上一次处理交易逻辑的bar的个数
    g.pl_bar_number = 0


## 开盘前运行函数
def pl_before_market_open(context):
    pass


def pl_trade(context):
    if g.pl_bar_number % PL_TRADE_BAR_DURATION == 0:
        pl_sell(context)   # 建仓
        pl_buy(context)
    g.pl_bar_number = (g.pl_bar_number + 1 ) % PL_TRADE_BAR_DURATION

## 收盘后运行函数
def pl_after_market_close(context):
    # 绘制当天的仓位
    pl_cash = context.portfolio.available_cash
    pl_total = context.portfolio.total_value
    pl_position = (pl_total - pl_cash)/pl_total * 100
    record(position=pl_position)

    ## 更新股票池
    if g.pl_stock_pool_update_day % PL_CHANGE_STOCK_POOL_DAY_NUMBER == 0:
        pl_stock_pool(context)
    g.pl_stock_pool_update_day = (g.pl_stock_pool_update_day + 1) % PL_CHANGE_STOCK_POOL_DAY_NUMBER
    return


def pl_load_fundamentals_data(context,pl_date):
    pl_current_date = pl_date.strftime("%Y-%m-%d")

    pl_df = get_fundamentals(query(valuation,indicator), pl_current_date)

    pl_raw_data = []

    for pl_index in range(len(pl_df['code'])):
        pl_code = pl_df['code'][pl_index]
        pl_raw_data_item = {'code':pl_code,'market_cap':pl_df['market_cap'][pl_index],'pe_ratio':pl_df['pe_ratio'][pl_index]}
        pl_raw_data.append(pl_raw_data_item)
    return pl_raw_data


def pl_load_change_pct_data(context,pl_date,pl_codes):
    pl_change_pct_dict_list = []
    # 计算涨跌幅需要用到前一日收盘价，所以需要多加载一天的数据，
    # 计算前一个交易日收盘后的数据，所以需要再多加载一天
    # 使用固定的25个交易日，而非25个bar计算涨跌幅
    pl_count = PL_CHANGE_PCT_DAY_NUMBER + 1
    # 获取25个交易日的日期
    pl_pre_25_dates = jqdata.get_trade_days(start_date=None, end_date=pl_date, count=pl_count)
    pl_pre_25_date = pl_pre_25_dates[0]
    pl_pre_1_date = pl_pre_25_dates[-1]
    for pl_code in pl_codes:
        pl_pre_25_data =  get_price(pl_code, start_date=None, end_date=pl_pre_25_date, frequency='daily', fields=['close'], skip_paused=True, fq='post', count=1)
        pl_pre_1_data =  get_price(pl_code, start_date=None, end_date=pl_pre_1_date, frequency='daily', fields=['close'], skip_paused=True, fq='post', count=1)
        pl_pre_25_close = None
        pl_pre_1_close = None
        if str(pl_pre_25_date) == str(pl_pre_25_data.index[0])[:10]:
            pl_pre_25_close = pl_pre_25_data['close'][0]
        if str(pl_pre_1_date) == str(pl_pre_1_data.index[0])[:10]:
            pl_pre_1_close = pl_pre_1_data['close'][0]

        if pl_pre_25_close != None and pl_pre_1_close != None and not math.isnan(pl_pre_25_close) and not math.isnan(pl_pre_1_close):
            pl_change_pct = (pl_pre_1_close - pl_pre_25_close) / pl_pre_25_close
            pl_item = {'code':pl_code, 'change_pct': pl_change_pct}
            pl_change_pct_dict_list.append(pl_item)
    return pl_change_pct_dict_list


def pl_stock_pool(context):
    '''
    更新股票池
    '''
    pl_date = context.current_dt
    pl_current_date = context.current_dt.strftime("%Y-%m-%d")

    # 获取股票财务数据
    pl_raw_data = pl_load_fundamentals_data(context,pl_date)

    # 剔除ST的股票
    pl_raw_data_array = []
    pl_current_datas = get_current_data()
    for pl_item in pl_raw_data:
        pl_code = pl_item['code']
        pl_current_data = pl_current_datas[pl_code]
        if pl_current_data.is_st:
            continue
        pl_name = pl_current_data.name
        if 'ST' in pl_name or '*' in pl_name or '退' in pl_name:
            continue
        pl_raw_data_array.append(pl_item)

    pl_raw_data = pl_raw_data_array

    # 按照财务信息中的总市值降序排序
    pl_raw_data = sorted(pl_raw_data,key = lambda item:item['market_cap'],reverse=True)
    # 剔除总市值排名最小的10%的股票
    pl_fitered_market_cap = pl_raw_data[:int(len(pl_raw_data) * 0.9)]

    # 剔除PE TTM 小于0或大于100的数据
    pl_filtered_pe = []
    for pl_stock in pl_fitered_market_cap:
        if pl_stock['pe_ratio'] == None or math.isnan(pl_stock['pe_ratio']) or float(pl_stock['pe_ratio']) < 0 or float(pl_stock['pe_ratio']) > 100:
            continue
        pl_filtered_pe.append(pl_stock['code'])

    pl_change_pct_dict_list = pl_load_change_pct_data(context,pl_date,pl_filtered_pe)
    # 按照涨跌幅升序排序
    pl_change_pct_dict_list = sorted(pl_change_pct_dict_list,key = lambda item:item['change_pct'],reverse=False)
    # 取跌幅前10%的股票
    pl_change_pct_dict_list = pl_change_pct_dict_list[0:(int(len(pl_change_pct_dict_list)*0.1))]

    # 获取最终的股票池
    pl_last_stock_pool = []
    for pl_stock in pl_change_pct_dict_list:
        pl_last_stock_pool.append(pl_stock['code'])

    log.info(pl_current_date + '调整股票池,筛选出的股票池：' + str(pl_last_stock_pool))
    g.pl_stock_pool = pl_last_stock_pool


def pl_buy(context):
    '''
    买入逻辑

    买入信号：MA3上穿MA200
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
        pl_close_data = attribute_history(security=pl_code, count=pl_count, unit=PL_UNIT,fields=['close'],skip_paused=True, df=True, fq='pre')['close']
        if (list(np.isnan(pl_close_data)).count(True) > 0) or (len(list(pl_close_data)) < pl_count):
            continue

        # MA3上穿MA200
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

    卖出信号：MA3下穿MA200
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
        pl_close_data = attribute_history(security=pl_code, count=pl_count, unit=PL_UNIT,fields=['close'],skip_paused=True, df=True, fq='pre')['close']

        if (list(np.isnan(pl_close_data)).count(True) > 0) or (len(list(pl_close_data)) < pl_count):
            continue

        # MA3下穿MA200
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

    使用等额仓位，买入初始资金的1/200 的仓位
    '''
    return context.portfolio.starting_cash / 200


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
        pl_code 标的的代码。如要检测平安银行需要传入参数 000001.XSHE
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

