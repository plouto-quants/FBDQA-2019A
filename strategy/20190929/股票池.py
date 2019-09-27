"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""
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


# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
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


'''
初始化全局变量
'''
def pl_init_global(context):
    # 距上一次股票池更新的天数
    g.pl_stock_pool_update_day = 0
    # 股票池，股票代码
    g.pl_stock_pool = []
    # 需要买入的股票池
    g.pl_need_buy_stock = []
    # 是否更新股票池
    g.pl_updated_stock_pool = False
    # 每只标的需要买入的头寸
    g.pl_position = 0


'''
开盘前运行函数
'''
def pl_before_market_open(context):
    pass


'''
交易函数
'''
def pl_trade(context):
    if not g.pl_updated_stock_pool:
        return
    # 调用卖出函数
    pl_sell(context)

    # 计算头寸
    if len(g.pl_need_buy_stock) > 0:
        g.pl_position = context.portfolio.available_cash / len(g.pl_need_buy_stock)
        log.info("股票池变动后需要买入的数量",len(g.pl_need_buy_stock),"可用现金",context.portfolio.available_cash,"每只股票的头寸",g.pl_position,"需要新买入的标的",g.pl_need_buy_stock)
    g.pl_updated_stock_pool = False

    pl_buy(context)   # 建仓
    pass


'''
收盘后处理

1. 更新股票池
2. 更新股票池后，获取需要新买入的标的列表
'''
def pl_after_market_close(context):
    if g.pl_stock_pool_update_day % PL_CHANGE_STOCK_POOL_DAY_NUMBER == 0:
        pl_old_stock_pool = g.pl_stock_pool
        # 更新股票池
        pl_stock_pool(context)
        # 获取需要新买入的标的列表
        g.pl_need_buy_stock = []
        for pl_code in g.pl_stock_pool:
            if pl_code not in pl_old_stock_pool:
                g.pl_need_buy_stock.append(pl_code)
        g.pl_updated_stock_pool = True
    g.pl_stock_pool_update_day = (g.pl_stock_pool_update_day + 1) % PL_CHANGE_STOCK_POOL_DAY_NUMBER

    record(pos=(context.portfolio.positions_value / context.portfolio.total_value * 100))
    pass


'''
卖出逻辑。
当标的不在股票池中时，卖出该标的的持仓。
'''
def pl_sell(context):
    for pl_code in context.portfolio.positions.keys():
        if pl_code not in g.pl_stock_pool:
            if pl_is_low_limit(pl_code):
                continue
            # 标的已经不在股票池中尝试卖出该标的的股票
            pl_order_ = order_target(security=pl_code, amount=0)
            if pl_order_ is not None and pl_order_.filled:
                log.info("交易 卖出",pl_code,pl_order_.filled)
    pass

'''
买入逻辑。
'''
def pl_buy(context):
    for pl_code in g.pl_stock_pool:
        if pl_code in context.portfolio.positions.keys():
            continue
        pl_current_data = get_current_data()[pl_code]
        if pl_current_data == None:
            return
        if pl_is_high_limit(pl_code):
            continue
        pl_order_ = order_value(security=pl_code, value=g.pl_position)
        if pl_order_ is not None and pl_order_.filled > 0:
            log.info("交易 买入",pl_code,"买入仓位",g.pl_position,"买入的股数",pl_order_.filled)
    pass


'''
加载股票的财务数据，包括总市值和PE
'''
def pl_load_fundamentals_data(context):
    pl_df = get_fundamentals(query(valuation,indicator), context.current_dt.strftime("%Y-%m-%d"))
    pl_raw_data = []
    for pl_index in range(len(pl_df['code'])):
        pl_raw_data_item = {
            'code'      :pl_df['code'][pl_index],
            'market_cap':pl_df['market_cap'][pl_index],
            'pe_ratio'  :pl_df['pe_ratio'][pl_index]
            }
        pl_raw_data.append(pl_raw_data_item)
    return pl_raw_data


'''
计算标的的25日涨跌幅。

Args:
    context 上下文
    pl_codes   标的的代码列表
Returns:
    标的的涨跌幅列表。列表中的每一项数据时一个字典：
        pl_code:标的代码
        pl_change_pct: 标的的涨跌幅
'''
def pl_load_change_pct_data(context,pl_codes):
    pl_change_pct_dict_list = []
    # 计算涨跌幅需要用到前一日收盘价，所以需要多加载一天的数据，
    # 计算前一个交易日收盘后的数据，所以需要再多加载一天
    # 使用固定的25个交易日，而非25个bar计算涨跌幅
    pl_count = PL_CHANGE_PCT_DAY_NUMBER + 1
    # 获取25个交易日的日期
    pl_pre_25_dates = jqdata.get_trade_days(start_date=None, end_date=context.current_dt, count=pl_count)
    pl_pre_25_date  = pl_pre_25_dates[0]
    pl_pre_1_date   = pl_pre_25_dates[-1]
    for pl_code in pl_codes:
        pl_pre_25_data = get_price(pl_code, start_date=None, end_date=pl_pre_25_date, frequency='daily', fields=['close'], skip_paused=True, fq='post', count=1)
        pl_pre_1_data  = get_price(pl_code, start_date=None, end_date=pl_pre_1_date, frequency='daily', fields=['close'], skip_paused=True, fq='post', count=1)
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



'''
更新股票池。该方法在收盘后调用。

1. 全市场的股票作为基础股票池
2. 在基础股票池的基础上剔除ST的股票作为股票池1
3. 在股票池1的基础上剔除总市值最小的10%的股票作为股票池2
4. 在股票池2的基础上剔除PE < 0 或 PE > 100的股票作为股票池3
5. 在股票池3的基础上 取25日跌幅前10%的股票作为最终的股票池
'''
def pl_stock_pool(context):
    pl_current_date = context.current_dt.strftime("%Y-%m-%d")
    # 获取股票财务数据
    pl_raw_data = pl_load_fundamentals_data(context)

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
    pl_fitered_market_cap = pl_raw_data[:int(len(pl_raw_data) *0.9 )]
    # 剔除PE TTM 小于0或大于100
    pl_filtered_pe = []
    for pl_stock in pl_fitered_market_cap:
        if pl_stock['pe_ratio'] == None or math.isnan(pl_stock['pe_ratio']) or float(pl_stock['pe_ratio']) < 0 or float(pl_stock['pe_ratio']) > 100:
            continue
        pl_filtered_pe.append(pl_stock['code'])

    # 加载标的的涨跌幅信息
    pl_change_pct_dict_list = pl_load_change_pct_data(context,pl_filtered_pe)
    # 按照涨跌幅升序排序
    pl_change_pct_dict_list = sorted(pl_change_pct_dict_list,key = lambda item:item['change_pct'],reverse=False)
    # 取跌幅前10%的股票
    pl_change_pct_dict_list = pl_change_pct_dict_list[0:(int(len(pl_change_pct_dict_list)*0.1))]

    # 获取最终的股票池
    g.pl_stock_pool = []
    for pl_stock in pl_change_pct_dict_list:
        g.pl_stock_pool.append(pl_stock['code'])
    log.info('调整股票池,筛选出的股票池：',g.pl_stock_pool)
    pass


'''
判断标的是否涨停或停牌。

Args:
    pl_code 标的的代码

Returns:
    True 标的涨停或停牌，该情况下无法买入
'''
def pl_is_high_limit(pl_code):
    pl_current_data = get_current_data()[pl_code]
    if pl_current_data.last_price >= pl_current_data.high_limit:
        return True
    if pl_current_data.paused:
        return True
    return False


'''
判断标的是否跌停或停牌。

Args:
    pl_code 标的的代码

Returns:
    True 标的跌停或停牌，该情况下无法卖出
'''
def pl_is_low_limit(pl_code):
    pl_current_data = get_current_data()[pl_code]
    if pl_current_data.last_price <= pl_current_data.low_limit:
        return True
    if pl_current_data.paused:
        return True
    return False
