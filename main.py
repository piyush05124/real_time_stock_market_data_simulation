import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from warnings import filterwarnings
filterwarnings('ignore')
from DATA_TRANSFORMER_LOADER.redis_retriever import (sec_to_minutes, bulk_insert_db, simple_buy_sell_with_pnl, r, getDateRangeData,generate_signals)
today_date = str(datetime.now().date())
# Streamlit setup
st.set_page_config(page_title="📈 Stock Market Dashboard", layout="wide")
st.title("📈 Real-Time & Historical Stock Market Dashboard")

symbol_table_mapping = {
    "USDINR": "df_usdinr_minute_data",
    "NIFTY": "df_nifty_minute_data"
}
symbols_list = list(symbol_table_mapping.keys())

# Sidebar for symbol and date range
st.sidebar.header("📅 Filter Options")
selected_symbol = st.sidebar.selectbox("Select Symbol", symbols_list)

# print(abs(date_range[0]-date_range[1]).days)
graph_type = st.sidebar.selectbox(
                                "Select Graph Type",
                                ["Candlestick", "Line", "Candlestick + Line"],
                                index=0  # default to Candlestick + Line
                            )


duration_list = ['1min','5min','10min','30min','60min','120min',
                                            '1D','5D','15D',
                                            '1M','1Q','1Y'
                                            ]
duration_type = st.sidebar.segmented_control('Select Duration',duration_list,default=duration_list[0])



st.session_state['duration']= duration_type



# Session state
if 'current_date' not in st.session_state:
    st.session_state['current_date'] = None

if 'label' not in st.session_state:
    st.session_state['label'] = None

if 'duration' not in st.session_state:
    st.session_state['duration'] = None

if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "📜 Historical Data"

if 'indicator' not in st.session_state:
    st.session_state['indicator'] = None

st.session_state['label'] = selected_symbol





# Tabs
tab1, tab2 = st.tabs(["🟢 Live Feed", "📜 Historical Data"])


date_range = st.sidebar.date_input("Select Date Range", value=[datetime.now() - timedelta(days=5), datetime.now()],
                                       disabled=(st.session_state.active_tab != "📜 Historical Data"))

checked = st.sidebar.checkbox('Show MA Indicator',value=True)

def trigger_db(df,symbols_list = symbols_list):
    for i in symbols_list:
        # print(i, df['symbol'].unique())
        
        # print(i, len(filter_data))
        if((len(df['symbol'].str.contains(i))%5)==0) and len(df)!=0 :
            # print(len(df['symbol'].str.contains(i)))
            filter_data = df[df['symbol'].str.contains(i)]
            bulk_insert_db(filter_data.dropna(),table = symbol_table_mapping[i])


# ============================== Live Feed Tab ==============================
with tab1:
    
    # print(duration_type)
    st.session_state.active_tab = "🟢 Live Feed"
    auto_refresh = st.toggle("Auto-refresh graph", value=False)
    if auto_refresh:
        st_autorefresh(interval=2000, limit=None, key="live_refresh")

    def get_minute_data():
        sym = st.session_state['label']
        current_date = today_date#st.session_state['current_date']
        # print(sym,current_date)
        if sym!=None:
            dt = r.lrange(f"{current_date}_{sym}",0,-1)
            df = pd.DataFrame([json.loads(i) for i in dt])

            # print(df)
            if not df.empty:
                df_db = sec_to_minutes(df,interval='1min',reset_index=False)
                df = sec_to_minutes(df,interval=st.session_state['duration'],reset_index=False)
                
                df.dropna(inplace=True)
                #==================#
                
                trigger_db(df_db)

                #==================#
                try:
                    df = generate_signals(df)
                    # print(df.tail(2))
                except:
                    pass

            return df
        else:
            return pd.DataFrame()

    def plot_live_graph():
        df = get_minute_data()
        fig = go.Figure()
        if not df.empty:
            if graph_type in ["Candlestick", "Candlestick + Line"]:
                fig.add_trace(go.Candlestick(
                    x=df.index,
                    open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'],
                    name='OHLC',
                    increasing_line_color='green',
                    decreasing_line_color='red'
                ))
            if graph_type in ["Line", "Candlestick + Line"]:
                fig.add_trace(go.Scatter(
                    x=df.index,
                    y=df['close'],
                    mode='lines',
                    name='Close Price',
                    line=dict(color='#4bf9f3', width=1)
                ))
            try:
                if checked:
                    if ('SMA_Short' in list(df.columns) )or ('SMA_Long' in df.columns):
                        fig.add_trace(go.Scatter(
                            x=df.index, y=df['SMA_Short'], mode='lines', name='Short MA',
                            line=dict(color='blue', width=1)
                        ))

                        fig.add_trace(go.Scatter(
                            x=df.index, y=df['SMA_Long'], mode='lines', name='Long MA',
                            line=dict(color='orange', width=1)
                        ))

                        buy_signals = df[df['Position'] == 1]
                        fig.add_trace(go.Scatter(
                            x=buy_signals.index,
                            y=buy_signals['close'],
                            mode='markers',
                            name='Buy',
                            marker=dict(symbol='triangle-up', color='green', size=25)
                        ))

                        sell_signals = df[df['Position'] == -1]
                        fig.add_trace(go.Scatter(
                            x=sell_signals.index,
                            y=sell_signals['close'],
                            mode='markers',
                            name='Sell',
                            marker=dict(symbol='triangle-down', color='red', size=25)
                        ))  
            except:
                pass
            fig.update_layout(title=f"{selected_symbol} - Live Feed", xaxis_rangeslider_visible=False)
            fig.update_xaxes(type='date', showgrid=True)
            fig.update_yaxes(autorange=True, showgrid=True)
        return fig

    st.plotly_chart(plot_live_graph(), use_container_width=True,key='live')

# ============================== Historical Tab ==============================
with tab2:
    
    st.session_state.active_tab = "📜 Historical Data"
    # print(st.session_state.active_tab )


    if(abs(date_range[0]-date_range[1]).days)>200:
        st.session_state['duration']= duration_list[6]
    def get_historical_data():
        
        table = symbol_table_mapping[selected_symbol]
        
        start_date, end_date = date_range
        df = getDateRangeData(table,start_date, end_date)
        if not df.empty:
            df = sec_to_minutes(df,interval=st.session_state['duration'],reset_index=True)
        return df
      

    def plot_historical_graph():
        if st.session_state.get('last_hist_symbol') != selected_symbol:
            st.session_state['last_hist_symbol'] = selected_symbol

        df = get_historical_data()#st.session_state['historical_df']
        fig = go.Figure()
        if not df.empty:
            df = simple_buy_sell_with_pnl(df)
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)

            if graph_type in ["Candlestick", "Candlestick + Line"]:
                fig.add_trace(go.Candlestick(
                    x=df.index,
                    open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'],
                    name='OHLC',
                    increasing_line_color='green',
                    decreasing_line_color='red'
                ))
            if graph_type in ["Line", "Candlestick + Line"]:
                fig.add_trace(go.Line(
                    x=df.index,
                    y=df['close'],
                    mode='lines',
                    name='Close Price',
                    line=dict(color='#4bf9f3', width=1)
                ))
            try:
                if checked:
                    if ('SMA_Short' in list(df.columns) )or ('SMA_Long' in df.columns):
                        fig.add_trace(go.Scatter(
                            x=df.index, y=df['SMA_Short'], mode='lines', name='Short MA',
                            line=dict(color='blue', width=1)
                        ))

                        fig.add_trace(go.Scatter(
                            x=df.index, y=df['SMA_Long'], mode='lines', name='Long MA',
                            line=dict(color='orange', width=1)
                        ))

                        buy_signals = df[df['Position'] == 1]
                        fig.add_trace(go.Scatter(
                            x=buy_signals.index,
                            y=buy_signals['close'],
                            mode='markers',
                            name='Buy',
                            marker=dict(symbol='triangle-up', color='green', size=25)
                        ))

                        sell_signals = df[df['Position'] == -1]
                        fig.add_trace(go.Scatter(
                            x=sell_signals.index,
                            y=sell_signals['close'],
                            mode='markers',
                            name='Sell',
                            marker=dict(symbol='triangle-down', color='red', size=25)
                        ))  
            except:
                pass
            fig.update_layout(title=f"{selected_symbol} - Historical Data", xaxis_rangeslider_visible=False)
            fig.update_xaxes(type='date', rangeslider_visible=False, showgrid=True)
            fig.update_yaxes(autorange=True, showgrid=True)

        return fig

    st.plotly_chart(plot_historical_graph(), use_container_width=True,key='hist')




# print(getDateRangeData('df_nifty_minute_data','2025-04-24','2025-04-25'))
