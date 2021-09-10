#!/usr/bin/env python
# coding: utf-8

# In[183]:


import pandas as pd
from datetime import datetime, timedelta
import requests


# ## country data
# * ベースとなる国データをダウンロード・作成

# In[96]:


countries = pd.read_csv('https://raw.githubusercontent.com/AnnaNishino/covid_automation/main/country_stats.csv',index_col=0)


# In[97]:


countries['Dev_j'] = countries.Classification.map({'Developing':'新興国','Developed':'先進国'})
countries.population = countries.population * 1000


# ## JHU Dashboard
# リアルタイムデータをAPI経由で取得

# In[98]:


data = requests.get(
    'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases2_v1/FeatureServer/2/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&resultOffset=0&resultRecordCount=200&cacheHint=true&quantizationParameters=%7B%22mode%22%3A%22edit%22%7D').json()


# In[99]:


country_list = []
for country in data['features']:
    country_dic = {}
    #country_dic['OBJECTID'] = country['attributes']['OBJECTID']
    #deleted in v2 #country_dic['Province_State'] = country['attributes']['Province_State']
    country_dic['Country_Region'] = country['attributes']['Country_Region']
    country_dic['Last_Update'] = country['attributes']['Last_Update']
    #country_dic['Lat'] = country['attributes']['Lat']
    #country_dic['Long_'] = country['attributes']['Long_']
    country_dic['Confirmed'] = country['attributes']['Confirmed']
    country_dic['Recovered'] = country['attributes']['Recovered']
    country_dic['Deaths'] = country['attributes']['Deaths']
    country_dic['Active'] = country['attributes']['Active']
    country_dic['Mortality_Rate'] = country['attributes']['Mortality_Rate']
    country_list.append(country_dic)


# In[100]:


df1 = pd.DataFrame(country_list)


# In[101]:


df1.Last_Update = pd.to_datetime(df1.Last_Update,unit='ms').dt.tz_localize('utc').dt.tz_convert('Asia/Tokyo').dt.date.astype('datetime64')


# In[102]:


data2 = requests.get(
    'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases2_v1/FeatureServer/3/query?f=json&'
    'where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&resultOffset=0'
    '&resultRecordCount=2000&cacheHint=true&quantizationParameters=%7B%22mode%22%3A%22edit%22%7D').json()


# In[103]:


state_list = []
for state in data2['features']:
    state_dic = {}
    #state_dic['OBJECTID'] = state['attributes']['OBJECTID']
    state_dic['Province_State'] = state['attributes']['Province_State']
    state_dic['Country_Region'] = state['attributes']['Country_Region']
    state_dic['Last_Update'] = state['attributes']['Last_Update']
    #state_dic['Lat'] = state['attributes']['Lat']
    #state_dic['Long_'] = state['attributes']['Long_']
    state_dic['Confirmed'] = state['attributes']['Confirmed']
    state_dic['Recovered'] = state['attributes']['Recovered']
    state_dic['Deaths'] = state['attributes']['Deaths']
    state_dic['Active'] = state['attributes']['Active']
    state_dic['Mortality_Rate'] = state['attributes']['Mortality_Rate']
    state_list.append(state_dic)


# In[104]:


df2 = pd.DataFrame(state_list)


# In[105]:


df2.Last_Update = pd.to_datetime(df2.Last_Update,unit='ms').dt.tz_localize('utc').dt.tz_convert('Asia/Tokyo').dt.date.astype('datetime64')


# In[106]:


df2.loc[df2[df2['Province_State'] == 'Hong Kong'].index,'Country_Region'] = 'Hong Kong'


# In[107]:


df2.loc[df2[df2['Province_State'] == 'Macau'].index,'Country_Region'] = 'Macau'


# In[108]:


df2[df2.Province_State.isin(['Hong Kong','Macau'])].drop('Province_State',axis=1)


# In[109]:


df = pd.concat([df1[~df1.Country_Region.isin(['China'])],
                df2[df2.Province_State.isin(['Hong Kong','Macau'])].drop('Province_State',axis=1),
                df2[df2.Country_Region.isin(['China'])].groupby(['Country_Region','Last_Update']).sum().reset_index()],ignore_index=True)


# In[110]:


df = df.set_index('Country_Region')
df.tail()


# In[111]:


confirmed = pd.read_csv(
    'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')


# In[112]:


confirmed.head()


# In[113]:


confirmed.loc[confirmed[confirmed['Province/State'] == 'Hong Kong'].index,'Country/Region'] = 'Hong Kong'


# In[114]:


confirmed.loc[confirmed[confirmed['Province/State'] == 'Macau'].index,'Country/Region'] = 'Macau'


# In[115]:


confirmed_df = confirmed.groupby('Country/Region').sum().drop(['Lat','Long'],axis=1)


# In[116]:


confirmed_df.columns = pd.to_datetime(confirmed_df.columns,format='%m/%d/%y').strftime('%Y-%m-%d')


# In[117]:


confirmed_df = pd.concat([confirmed_df,
                          df.groupby('Country_Region').sum().Confirmed.rename((datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'))],
                         axis=1,sort=True).fillna(0).astype(int)


# In[118]:


new_cases = confirmed_df.diff(axis=1).fillna(0).astype(int)
new_cases_7dma = new_cases.drop(new_cases.columns[-1],axis=1).rolling(window=7,axis=1).mean().dropna(axis=1).sort_values(new_cases.drop(new_cases.columns[-1],axis=1).columns[-1],ascending=False)


# In[119]:


deaths = pd.read_csv(
    'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')


# In[120]:


deaths.loc[deaths[deaths['Province/State'] == 'Hong Kong'].index,'Country/Region'] = 'Hong Kong'
deaths.loc[deaths[deaths['Province/State'] == 'Macau'].index,'Country/Region'] = 'Macau'


# In[121]:


deaths_df = deaths.groupby('Country/Region').sum().drop(['Lat','Long'],axis=1)


# In[122]:


deaths_df.columns = pd.to_datetime(deaths_df.columns,format='%m/%d/%y').strftime('%Y-%m-%d')


# In[123]:


deaths_df = pd.concat([deaths_df,
                       df.groupby('Country_Region').sum().Deaths.rename((datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'))],
                      axis=1,sort=True).fillna(0).astype(int)


# In[124]:


new_deaths = deaths_df.diff(axis=1).fillna(0).astype(int)
new_deaths_7dma = new_deaths.drop(new_deaths.columns[-1],axis=1).rolling(window=7,axis=1).mean().dropna(axis=1).sort_values(new_deaths.drop(new_deaths.columns[-1],axis=1).columns[-1],ascending=False)




# In[131]:


confirmed_df.index = confirmed_df.index.map(countries.set_index('Country').Japanese.to_dict())
new_cases.index = new_cases.index.map(countries.set_index('Country').Japanese.to_dict())
deaths_df.index = deaths_df.index.map(countries.set_index('Country').Japanese.to_dict())
new_deaths.index = new_deaths.index.map(countries.set_index('Country').Japanese.to_dict())

# In[132]:


summary_df = pd.concat(
    [confirmed_df.sum().rename('confirmed'),deaths_df.sum().rename('deaths'),
     new_cases.sum().astype(int).rename('new_cases'),new_deaths.sum().astype(int).rename('new_deaths')],axis=1)

summary_df.columns = summary_df.columns.map(
    {'confirmed':'累積感染者数','deaths':'累積死者数',
     'new_cases':'新規感染者数','new_deaths':'新規死者数'})


# In[133]:


summary_df.to_csv('./data/summary.csv', encoding='utf_8_sig')
confirmed_df.to_csv('./data/confirmed.csv', encoding='utf_8_sig')
deaths_df.to_csv('./data/deaths.csv', encoding='utf_8_sig')
new_cases.to_csv('./data/new_cases.csv', encoding='utf_8_sig')
new_deaths.to_csv('./data/new_deaths.csv', encoding='utf_8_sig')

# #### confirmed

# In[134]:


confirmed = pd.read_csv(
    'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')


# In[135]:


confirmed.loc[confirmed[confirmed['Province/State'] == 'Hong Kong'].index,'Country/Region'] = 'Hong Kong'


# In[136]:


confirmed.loc[confirmed[confirmed['Province/State'] == 'Macau'].index,'Country/Region'] = 'Macau'


# In[137]:


confirmed_df = confirmed.groupby('Country/Region').sum().drop(['Lat','Long'],axis=1)


# In[138]:


confirmed_df.columns = pd.to_datetime(confirmed_df.columns,format='%m/%d/%y').strftime('%Y-%m-%d')


# In[139]:


confirmed_df.index = confirmed_df.index.map(countries.set_index('Country').Japanese.to_dict())


# In[140]:


confirmed_df = confirmed_df.T
confirmed_df.head()


# In[141]:


confirmed_card = pd.concat([confirmed_df.iloc[-1].rename('累計感染者数'),
                           (confirmed_df / confirmed_df.columns.map(countries.set_index('Japanese').population.to_dict()) * 1000).iloc[-1].rename('累計感染者数（人口1000人あたり）')],
                            axis=1).rename_axis('国・地域')
confirmed_card.head()


# In[142]:


confirmed_card.to_csv('./data/for_gds/confirmed_card.csv', encoding='utf_8_sig')
pd.melt(confirmed_df.reset_index(),id_vars='index').to_csv('./data/for_gds/confirmed_ts.csv', encoding='utf_8_sig')


# 
# #### deaths

# In[143]:


deaths = pd.read_csv(
    'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')


# In[144]:


deaths.loc[deaths[deaths['Province/State'] == 'Hong Kong'].index,'Country/Region'] = 'Hong Kong'


# In[145]:


deaths.loc[deaths[deaths['Province/State'] == 'Macau'].index,'Country/Region'] = 'Macau'


# In[146]:


deaths_df = deaths.groupby('Country/Region').sum().drop(['Lat','Long'],axis=1)


# In[147]:


deaths_df.columns = pd.to_datetime(deaths_df.columns,format='%m/%d/%y').strftime('%Y-%m-%d')


# In[148]:


deaths_df.index = deaths_df.index.map(countries.set_index('Country').Japanese.to_dict())


# In[149]:


deaths_df = deaths_df.T
deaths_df.head()


# In[150]:


deaths_card = pd.concat([deaths_df.iloc[-1].rename('累計死者数'),
                           (deaths_df / deaths_df.columns.map(countries.set_index('Japanese').population.to_dict()) * 1000).iloc[-1].rename('累計死者数（人口1000人あたり）')],
                            axis=1).rename_axis('国・地域')
deaths_card.head()


# In[151]:


deaths_card.to_csv('./data/for_gds/deaths_card.csv', encoding='utf_8_sig')
pd.melt(deaths_df.reset_index(),id_vars='index').to_csv('./data/for_gds/deaths_ts.csv', encoding='utf_8_sig')




latest = pd.concat([confirmed_df.T[confirmed_df.T.columns[-1]].rename('Confirmed'),
                    deaths_df.T[deaths_df.T.columns[-1]].rename('Deaths'),
                    new_cases.iloc[-1].astype(int).rename('New Cases'),
                    new_deaths.iloc[-1].astype(int).rename('New Deaths'),
                   (new_cases.T.iloc[:,-7:].sum(axis=1) / new_cases.T.iloc[:,-14:-7].sum(axis=1)).rename('week by week')],axis=1)


# In[165]:


latest.head()


# In[166]:


latest['Mortality Rate'] = latest['Deaths'] / latest['Confirmed']*100


# In[167]:


latest.columns = latest.columns.map(
    {'Confirmed':'累計感染者数','Deaths':'累計死者数',
     'New Cases':'新規感染者数','New Deaths':'新規死者数',
    'Mortality Rate':'致死率','week by week':'一週間累計比'})


# In[168]:


latest.head()


# In[169]:


latest.to_csv('./data/daily_card.csv', encoding='utf_8_sig')


# ## 7-day average

# In[170]:


new_cases_7average = new_cases.rolling(window=7).mean().dropna()
new_cases_7average.head()


# In[171]:


new_cases_per_100k = new_cases_7average / new_cases_7average.columns.map(
    countries.set_index('Japanese').population.to_dict()) * 1000


# In[172]:


nc_melt = pd.melt(new_cases_7average.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases')


# In[173]:


nc_melt['country'] = nc_melt.country_j.map(countries.set_index('Japanese').Country.to_dict())
nc_melt['dev_j'] = nc_melt.country_j.map(countries.set_index('Japanese').Dev_j.to_dict())
nc_melt['region_j'] = nc_melt.country_j.map(countries.set_index('Japanese').Region_j.to_dict())


# In[174]:


nc_melt.head()


# In[175]:


new_deaths_7average = new_deaths.rolling(window=7).mean().dropna()
new_deaths_7average.head()


# In[176]:


new_deaths_per_100k = new_deaths_7average / new_deaths_7average.columns.map(
    countries.set_index('Japanese').population.to_dict()) * 100


# In[177]:


nd_melt = pd.melt(new_deaths_7average.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases')


# In[178]:


nd_melt['country'] = nd_melt.country_j.map(countries.set_index('Japanese').Country.to_dict())
nd_melt['dev_j'] = nd_melt.country_j.map(countries.set_index('Japanese').Dev_j.to_dict())
nd_melt['region_j'] = nd_melt.country_j.map(countries.set_index('Japanese').Region_j.to_dict())


# In[179]:


nd_melt


# In[180]:


pd.merge(nc_melt, pd.melt(new_cases_per_100k.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases_1k'),
         on=['date','country_j'], how='outer').to_csv('./data/for_gds/new_cases_7dma.csv', encoding='utf_8_sig')


# In[181]:


pd.merge(nd_melt, pd.melt(new_deaths_per_100k.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases_1k'),
         on=['date','country_j'], how='outer').to_csv('./data/for_gds/new_deaths_7dma.csv', encoding='utf_8_sig')


# In[ ]:




