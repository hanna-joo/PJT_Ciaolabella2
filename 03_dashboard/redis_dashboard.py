from rediscluster import RedisCluster
from streamlit_settings import redis_nodes, redis_password
import time
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ------------ get data and make to dataframe
client = RedisCluster(startup_nodes=redis_nodes, password=redis_password, decode_responses=True)
#date = time.strftime('%Y%m%d', time.localtime(time.time()))
date = '20221220'
# ------ common data
rows = list()
for key in client.scan_iter(match=f'common:{date}:*', count=100):
    temp, splitted = dict(), key.split(':')
    temp['date'], temp['hour'] = splitted[1], splitted[2]
    temp['user_gender'], temp['user_age'], temp['user_region'] = splitted[3], splitted[4], splitted[5]
    temp.update(client.hgetall(key))
    rows.append(temp)
df = pd.DataFrame(rows)
df = df.apply(pd.to_numeric, errors='ignore')
# ------ search data
rows = list()
for key in client.scan_iter(match=f'search:{date}:*', count=100):
    temp, splitted = dict(), key.split(':')
    temp['date'], temp['user_gender'], temp['user_age'] = splitted[1], splitted[2], splitted[3]
    temp['search_word'], temp['count'] = splitted[4], int(client.hget(key, 'count'))
    rows.append(temp)
df2 = pd.DataFrame(rows)
df2 = df2.apply(pd.to_numeric, errors='ignore')

# ------------ base
st.set_page_config(page_title="Ciaolabella2 Dashboard",
                   page_icon=":bar_chart:",
                   layout="wide")
st.write(f'# DATE : {date}')

# ------------ make tabs
tab1, tab2, tab3, tab4 = st.tabs(["IN OUT", "MENU", "ECO POINT", "NO LABEL"])

# ------------ data filter
st.sidebar.header("Filter")

hour = st.sidebar.multiselect(
    "Select Hour:",
    options=df['hour'].unique(),
    default=df['hour'].unique()
)
age = st.sidebar.multiselect(
    "Select Age:",
    options=df['user_age'].unique(),
    default=df['user_age'].unique()
)
gender = st.sidebar.multiselect(
    "Select Gender:",
    options=df['user_gender'].unique(),
    default=df['user_gender'].unique()
)
region = st.sidebar.multiselect(
    "Select Region:",
    options=df['user_region'].unique(),
    default=df['user_region'].unique()
)
selected = df.query(
    "hour == @hour & user_age == @age & user_gender == @gender & user_region == @region"
)
selected2 = df2.query(
    "user_age == @age & user_gender == @gender"
)

# ------------ data download
st.markdown("---")
st.markdown('#### 🔻 Common 데이터 확인 및 다운로드')
with st.expander("Click Here"):
    st.write(selected)
    st.download_button(
        label="Download .csv",
        data=selected.to_csv().encode('utf-8'),
        file_name=f'ciaolabella_common_{date}.csv',
        mime='text/csv'
    )
st.markdown('#### 🔻 Search 데이터 확인 및 다운로드')
with st.expander("Click Here"):
    st.write(selected2)
    st.download_button(
        label="Download .csv",
        data=selected2.to_csv().encode('utf-8'),
        file_name=f'ciaolabella_search_{date}.csv',
        mime='text/csv'
    )

# ------------ tab1 : login and logout
with tab1:
    item1, item2, item3 = st.columns(3)
    most_login_hour = (selected.groupby("hour")["login"].sum().sort_values().index[-1])
    most_login_age = (selected.groupby("user_age")["login"].sum().sort_values().index[-1])
    most_login_region = (selected.groupby("user_region")["login"].sum().sort_values().index[-1])
    with item1:
        st.markdown("#### 가장 많이 로그인한 시간")
        st.markdown(f"# <div style='text-align: center;'>{most_login_hour}시 </div>", unsafe_allow_html=True)
    with item2:
        st.markdown("#### 가장 많이 로그인한 연령")
        st.markdown(f"# <div style='text-align: center;'>{most_login_age}대 </div>", unsafe_allow_html=True)
    with item3:
        st.markdown("#### 가장 많이 로그인한 지역")
        st.markdown(f"# <div style='text-align: center;'>{most_login_region} </div>", unsafe_allow_html=True)
    st.markdown("---")

    item4, item5, item6 = st.columns([1,1,1])
    with item4:
        st.markdown("#### <div style='text-align: left;'>시간대별 로그인 및 로그아웃</div>", unsafe_allow_html=True)
        inout_by_hour = (
            selected.groupby(by=["hour"])[["login", "logout"]].sum().sort_index()
        )
        line1 = go.Figure()
        line1.add_trace(go.Scatter(
            x=inout_by_hour.index,
            y=inout_by_hour["login"],
            name='<b>login</b>',
            line=dict(color='Salmon', width=5),
        ))
        line1.add_trace(go.Scatter(
            x=inout_by_hour.index,
            y=inout_by_hour["logout"],
            name='<b>logout</b>',
            line=dict(color='CornflowerBlue', width=5),
        ))
        line1.update_layout(margin=dict(t=0, l=10, r=10, b=0))
        st.plotly_chart(line1)
    with item5:
        st.markdown('#### <div style="text-align: left;">연령대별 로그인</div>', unsafe_allow_html=True)
        login_by_age = (
            selected.groupby(by=["user_age"])["login"].sum().sort_index()
        )
        bar1 = px.bar(
            login_by_age,
            x=login_by_age.values,
            y=login_by_age.index,
            # color_discrete_sequence=['#0083B8'] * len(eco1_by_hour),
            color_discrete_sequence=["Salmon"],
            template='plotly_white',
            orientation='h',
            labels={'x': 'count'}
        )
        bar1.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(bar1)
    with item6:
        st.markdown('#### <div style="text-align: left;">성별 로그인</div>', unsafe_allow_html=True)
        login_by_gender = (
            selected.groupby(by=["user_gender"])["login"].sum()
        )
        pie1 = go.Figure(go.Pie(
            labels=login_by_gender.index,
            values=login_by_gender.values,
            hole=.3,
            marker_colors=px.colors.sequential.Sunset
        ))
        pie1.update_layout(
            margin=dict(t=0, l=0, r=0, b=0),
            font = dict(family="Arial", size=25, color="#000000")
        )
        st.plotly_chart(pie1)

# ------------ tab2 : menu click
with tab2:
    item1, item2 = st.columns(2)
    with item1:
        st.markdown('#### 연령대별 서비스 클릭')
        menu_by_age = (
            selected[["user_age", "menu_eco1", "menu_eco2", "menu_nolabel", "menu_lesswaste"]].groupby(
                by=["user_age"]).sum()
        ).stack().reset_index()
        menu_by_age.columns = ["age", "menu", "click"]
        menu_by_age["menu"] = menu_by_age["menu"].apply(lambda x: x.split('_')[1].upper())
        funnel1 = px.funnel(
            menu_by_age,
            x='click',
            y='menu',
            color='age',
            color_discrete_sequence=px.colors.sequential.Sunset
        )
        st.plotly_chart(funnel1)
    with item2:
        st.markdown('#### 성별 및 연령대별 선호 서비스')
        menu_by_gender_age = (
            selected[["user_gender", "user_age", "menu_eco1", "menu_eco2", "menu_nolabel", "menu_lesswaste"]].groupby(
                by=["user_gender", "user_age"]).sum()
        ).stack().reset_index()
        menu_by_gender_age.columns = ["gender", "age", "menu", "click"]
        menu_by_gender_age["menu"] = menu_by_gender_age["menu"].apply(lambda x: x.split('_')[1].upper())
        sunburst1 = px.sunburst(
            menu_by_gender_age,
            path=["gender", "age", "menu"],
            values="click",
            color_discrete_sequence=px.colors.sequential.Sunset
        )
        sunburst1.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(sunburst1)

    st.markdown('#### 메뉴 가수요 대비 실수요 (%)')
    st.text('각 서비스 메뉴를 클릭(가수요)한 후 실제로 해당 서비스를 이용(실수요)했는지를 백분율로 나타낸 그래프')
    item3, item4 = st.columns(2)
    with item3:
        eco1_demand = selected['ecopoint1_click'].sum() / selected['menu_eco1'].sum()
        gauge1 = go.Figure(go.Indicator(
            title={"text": "ECOPOINT 적립1"},
            value=round(eco1_demand * 100),
            number={"suffix": "%"},
            mode="gauge+number",
            gauge={
                "axis": {"range": [None, 100]},
                "bar": {"color": "Salmon"}
            }
        ))
        st.plotly_chart(gauge1)
    with item4:
        eco2_demand = selected['ecopoint2_click'].sum() / selected['menu_eco2'].sum()
        gauge2 = go.Figure(go.Indicator(
            title={"text": "ECOPOINT 적립2"},
            value=round(eco2_demand * 100),
            number={"suffix": "%"},
            mode="gauge+number",
            gauge={
                "axis": {"range": [None, 100]},
                "bar": {"color": "CornflowerBlue"}
            }
        ))
        st.plotly_chart(gauge2)

    item5, item6 = st.columns(2)
    with item5:
        nolabel_demand = selected['nolabel_click'].sum() / selected['menu_nolabel'].sum()
        gauge3 = go.Figure(go.Indicator(
            title={"text": "NO LABEL 제품 검색"},
            value=round(nolabel_demand * 100),
            number={"suffix": "%"},
            mode="gauge+number",
            gauge={
                "axis": {"range": [None, 100]},
                "bar": {"color": "CornflowerBlue"}
            }
        ))
        st.plotly_chart(gauge3)
    with item6:
        lesswaste_demand = selected['lesswaste_click'].sum() / selected['menu_lesswaste'].sum()
        gauge4 = go.Figure(go.Indicator(
            title={"text": "LESS WASTE 위치 검색"},
            value=round(lesswaste_demand * 100),
            number={"suffix": "%"},
            mode="gauge+number",
            gauge={
                "axis": {"range": [None, 100]},
                "bar": {"color": "Salmon"}
            },
        ))
        st.plotly_chart(gauge4)

# ------------ tab3 : ecopoint service
with tab3:
    item1, item2 = st.columns(2)
    with item1:
        st.markdown("#### 시간대별 ECOPOINT 추이")
        ecopoint_by_hour = selected[["hour", "save_ecopoint1", "save_ecopoint2"]].groupby(by=["hour"]).sum()
        bar2 = px.bar(
            ecopoint_by_hour,
            x=ecopoint_by_hour.index,
            y=["save_ecopoint1", "save_ecopoint2"],
            color_discrete_sequence=["#FFCC80", "Salmon"],
        )
        bar2.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(bar2)
    with item2:
        st.markdown("#### 연령대별 ECOPOINT 산점도")
        ecopoint_by_age = selected[["user_age", "save_ecopoint1", "save_ecopoint2"]]
        ecopoint_by_age["user_age"] = ecopoint_by_age["user_age"].astype("int")
        bubble1 = px.scatter(
            selected[["user_age", "save_ecopoint1", "save_ecopoint2"]],
            x="save_ecopoint1",
            y="save_ecopoint2",
            size="user_age",
            color="user_age",
            color_continuous_scale=px.colors.sequential.Sunset
        )
        bubble1.update_layout(yaxis_range=[10, 200], margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(bubble1)

    item3, item4 = st.columns(2)
    with item3:
        st.markdown("#### 지역별 ECOPOINT 적립량")
        ecopoint_by_region = selected[selected["user_gender"]!="none"]
        ecopoint_by_region["save_ecopoint"] = ecopoint_by_region["save_ecopoint1"] + ecopoint_by_region["save_ecopoint2"]
        area1 = px.area(
            ecopoint_by_region,
            x="hour",
            y="save_ecopoint",
            color="user_region",
            color_discrete_sequence=px.colors.sequential.Sunset,
        )
        area1.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(area1)
    with item4:
        st.markdown("#### 성별 ECOPOINT 적립량")
        ecopoint_by_gender = ecopoint_by_region
        hist1 = px.histogram(
            ecopoint_by_gender,
            x="hour",
            y="save_ecopoint",
            color="user_gender",
            marginal="box",
            color_discrete_sequence=["#FFC6AF", "Salmon"],
            hover_data=selected.columns
        )
        hist1.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(hist1)

# ------------ tab4 : nolabel service
with tab4:
    item1, item2, item3 = st.columns(3)
    with item1:
        st.markdown("#### NOLABEL 방문 유저 비율")
        condition = selected['user_gender'] == 'none'
        nouser = sum(selected.loc[condition].nolabel_click)
        user = sum(selected.loc[~condition].nolabel_click)
        pie2 = go.Figure(go.Pie(
            labels=["Users", "No Users"],
            values=[user, nouser],
            textinfo='label+percent',
            insidetextorientation='radial',
            hole=.3,
            marker_colors=["#FFCC80", "Salmon"],
            ))
        pie2.update_layout(
            margin=dict(t=0, l=0, r=0, b=0),
            font=dict(family="Arial", size=15, color="#000000")
        )
        st.plotly_chart(pie2)
    with item2:
        st.markdown("#### NOLABEL 시간대별 사용 비율")
        click_search = (
            selected[["hour", "nolabel_click", "nolabel_search"]].groupby(by=["hour"]).sum()
        ).stack().reset_index()
        click_search.columns = ["hour", "type", "count"]
        click_search["type"] = click_search["type"].apply(lambda x: x.split("_")[1])
        bar3 = px.bar(
            click_search,
            x="hour",
            y="count",
            color="type",
            color_discrete_sequence=["#FFC6AF", "Salmon"],
            barmode="group"
        )
        bar3.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(bar3)
    with item3:
        st.markdown("#### NOLABEL 검색어")
        tree1 = px.treemap(
            selected2,
            path=['user_gender', 'user_age', 'search_word'],
            values='count',
            color_discrete_sequence=px.colors.sequential.Sunset,
        )
        tree1.update_traces(
            root_color="Salmon"
        )
        tree1.update_layout(
            margin=dict(t=0, l=15, r=15, b=0),
            font=dict(family="Arial", size=25, color="#000000")
        )
        st.plotly_chart(tree1)
