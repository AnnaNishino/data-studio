#!/usr/bin/env python
# coding: utf-8

# In[76]:


import pandas as pd
from datetime import datetime, timedelta
import requests


# ## country data
# * ベースとなる国データをダウンロード・作成

# In[109]:


countries = pd.read_csv('https://raw.githubusercontent.com/AnnaNishino/covid_automation/main/country_stats.csv',index_col=0)


# In[110]:


countries['Dev_j'] = countries.Classification.map({'Developing':'新興国','Developed':'先進国'})
countries.population = countries.population * 1000


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


new_cases = confirmed_df.diff(axis=1).fillna(0).astype(int)
new_cases_7dma = new_cases.drop(new_cases.columns[-1],axis=1).rolling(window=7,axis=1).mean().dropna(axis=1).sort_values(new_cases.drop(new_cases.columns[-1],axis=1).columns[-1],ascending=False)


# In[118]:


deaths = pd.read_csv(
    'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')


# In[119]:


deaths.loc[deaths[deaths['Province/State'] == 'Hong Kong'].index,'Country/Region'] = 'Hong Kong'
deaths.loc[deaths[deaths['Province/State'] == 'Macau'].index,'Country/Region'] = 'Macau'


# In[120]:


deaths_df = deaths.groupby('Country/Region').sum().drop(['Lat','Long'],axis=1)


# In[121]:


deaths_df.columns = pd.to_datetime(deaths_df.columns,format='%m/%d/%y').strftime('%Y-%m-%d')


# In[122]:


new_deaths = deaths_df.diff(axis=1).fillna(0).astype(int)
new_deaths_7dma = new_deaths.drop(new_deaths.columns[-1],axis=1).rolling(window=7,axis=1).mean().dropna(axis=1).sort_values(new_deaths.drop(new_deaths.columns[-1],axis=1).columns[-1],ascending=False)


# In[123]:


#recovered = pd.read_csv(
#    'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')


# In[124]:


#recovered.loc[recovered[recovered['Province/State'] == 'Hong Kong'].index,'Country/Region'] = 'Hong Kong'
#recovered.loc[recovered[recovered['Province/State'] == 'Macau'].index,'Country/Region'] = 'Macau'
#recovered_df = recovered.groupby('Country/Region').sum().drop(['Lat','Long'],axis=1)
#recovered_df.columns = pd.to_datetime(recovered_df.columns,format='%m/%d/%y').strftime('%Y-%m-%d')


# In[125]:


#new_recovered = recovered_df.diff(axis=1).fillna(0).astype(int)


# In[126]:


#active_df = confirmed_df - deaths_df - recovered_df
#active_df.head()


# In[127]:


#active_change = active_df.diff(axis=1).fillna(0).astype(int)
#active_change.head()


# In[128]:


confirmed_df.index = confirmed_df.index.map(countries.set_index('Country').Japanese.to_dict())
new_cases.index = new_cases.index.map(countries.set_index('Country').Japanese.to_dict())
deaths_df.index = deaths_df.index.map(countries.set_index('Country').Japanese.to_dict())
new_deaths.index = new_deaths.index.map(countries.set_index('Country').Japanese.to_dict())
#recovered_df.index = recovered_df.index.map(countries.set_index('Country').Japanese.to_dict())
#new_recovered.index = new_recovered.index.map(countries.set_index('Country').Japanese.to_dict())
#active_df.index = active_df.index.map(countries.set_index('Country').Japanese.to_dict())
#active_change.index = active_change.index.map(countries.set_index('Country').Japanese.to_dict())


# In[129]:


summary_df = pd.concat(
    [confirmed_df.sum().rename('confirmed'),
     deaths_df.sum().rename('deaths'),
     #recovered_df.sum().rename('recovered'),
     #active_df.sum().rename('active'),
     new_cases.sum().astype(int).rename('new_cases'),
     new_deaths.sum().astype(int).rename('new_deaths'),
     #new_recovered.sum().astype(int).rename('new_recovered'),
     #active_change.sum().astype(int).rename('active_change')
    ],axis=1)

summary_df.columns = summary_df.columns.map(
    {'confirmed':'累積感染者数','deaths':'累積死者数',
     #'recovered':'累積回復者数','active':'現行感染者数',
     'new_cases':'新規感染者数','new_deaths':'新規死者数',
     #'new_recovered':'新規回復者数','active_change':'現行感染者増減数'
    })


# In[130]:


summary_df.to_csv('/Users/anna/Documents/covid19/data-studio/data/summary.csv', encoding='utf_8_sig')
confirmed_df.to_csv('/Users/anna/Documents/covid19/data-studio/data/confirmed.csv', encoding='utf_8_sig')
deaths_df.to_csv('/Users/anna/Documents/covid19/data-studio/data/deaths.csv', encoding='utf_8_sig')
#recovered_df.to_csv('/Users/anna/Documents/covid19/data-studio/data/recovered.csv', encoding='utf_8_sig')
#active_df.to_csv('/Users/anna/Documents/covid19/data-studio/data/active.csv', encoding='utf_8_sig')
new_cases.to_csv('/Users/anna/Documents/covid19/data-studio/data/new_cases.csv', encoding='utf_8_sig')
new_deaths.to_csv('/Users/anna/Documents/covid19/data-studio/data/new_deaths.csv', encoding='utf_8_sig')
#new_recovered.to_csv('/Users/anna/Documents/covid19/data-studio/data/new_recovered.csv', encoding='utf_8_sig')
#active_change.to_csv('/Users/anna/Documents/covid19/data-studio/data/active_change.csv', encoding='utf_8_sig')


# In[131]:


#active_change = active_df.diff().fillna(0).astype(int)


# In[132]:


confirmed_df = confirmed_df.T
confirmed_df.head()


# In[133]:


confirmed_card = pd.concat([confirmed_df.iloc[-1].rename('累計感染者数'),
                           (confirmed_df / confirmed_df.columns.map(countries.set_index('Japanese').population.to_dict()) * 1000).iloc[-1].rename('累計感染者数（人口1000人あたり）')],
                            axis=1).rename_axis('国・地域')
confirmed_card.head()


# In[134]:


confirmed_card.to_csv('./data/for_gds/confirmed_card.csv', encoding='utf_8_sig')
pd.melt(confirmed_df.reset_index(),id_vars='index').to_csv('./data/for_gds/confirmed_ts.csv', encoding='utf_8_sig')


# In[135]:


deaths_df = deaths_df.T
deaths_df.head()


# In[136]:


deaths_card = pd.concat([deaths_df.iloc[-1].rename('累計死者数'),
                           (deaths_df / deaths_df.columns.map(countries.set_index('Japanese').population.to_dict()) * 1000).iloc[-1].rename('累計死者数（人口1000人あたり）')],
                            axis=1).rename_axis('国・地域')
deaths_card.head()


# In[137]:


deaths_card.to_csv('./data/for_gds/deaths_card.csv', encoding='utf_8_sig')
pd.melt(deaths_df.reset_index(),id_vars='index').to_csv('./data/for_gds/deaths_ts.csv', encoding='utf_8_sig')


# 
# 
# 
# ## Daily mortality & infection rate + connect to GDS

# In[138]:


latest = pd.concat([confirmed_df.iloc[-1].rename('Confirmed'),
                    deaths_df.iloc[-1].rename('Deaths'),
                    #recovered_df.T[recovered_df.T.columns[-1]].rename('Recovered'),
                    #active_df.T[active_df.T.columns[-1]].rename('Active'),
                    new_cases.iloc[:,-1].astype(int).rename('New Cases'),
                    new_deaths.iloc[:,-1].astype(int).rename('New Deaths'),
                    #new_recovered.iloc[-1].astype(int).rename('New Recovered'),
                    #active_change.iloc[-1].astype(int).rename('Active Change'),
                    #(new_cases.T.iloc[:,-7:].sum(axis=1) / new_cases.T.iloc[:,-14:-7].sum(axis=1)).rename('week by week')
                   ],axis=1)


# In[139]:


latest.head()


# In[140]:


latest['Mortality Rate'] = latest['Deaths'] / latest['Confirmed']*100


# In[141]:


latest.columns = latest.columns.map(
    {'Confirmed':'累計感染者数','Deaths':'累計死者数',
     #'Recovered':'累計回復者数','Active':'現行感染者数',
     'New Cases':'新規感染者数','New Deaths':'新規死者数',
     #'New Recovered':'新規回復者数','Active Change':'現行感染者増減数',
    'Mortality Rate':'致死率','week by week':'一週間累計比'})


# In[142]:


latest.head()


# In[143]:


latest.to_csv('/Users/anna/Documents/covid19/data-studio/data/daily_card.csv', encoding='utf_8_sig')


# In[144]:


confirmed_card = pd.concat([confirmed_df.iloc[-1].rename('累計感染者数'),
                           (confirmed_df / confirmed_df.columns.map(countries.set_index('Japanese').population.to_dict()) * 1000).iloc[-1].rename('累計感染者数（人口1000人あたり）')],
                            axis=1).rename_axis('国・地域')
confirmed_card.head()


# ## 7-day average

# In[145]:


new_cases_7average = new_cases.T.rolling(window=7).mean().dropna()
new_cases_7average.head()


# In[146]:


new_cases_per_100k = new_cases_7average / new_cases_7average.columns.map(
    countries.set_index('Japanese').population.to_dict()) * 1000


# In[147]:


nc_melt = pd.melt(new_cases_7average.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases')


# In[148]:


nc_melt['country'] = nc_melt.country_j.map(countries.set_index('Japanese').Country.to_dict())
nc_melt['dev_j'] = nc_melt.country_j.map(countries.set_index('Japanese').Dev_j.to_dict())
nc_melt['region_j'] = nc_melt.country_j.map(countries.set_index('Japanese').Region_j.to_dict())


# In[149]:


nc_melt.head()


# In[150]:


new_deaths_7average = new_deaths.T.rolling(window=7).mean().dropna()
new_deaths_7average.tail()


# In[151]:


new_deaths_per_100k = new_deaths_7average / new_deaths_7average.columns.map(
    countries.set_index('Japanese').population.to_dict()) * 100


# In[152]:


nd_melt = pd.melt(new_deaths_7average.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases')


# In[153]:


nd_melt['country'] = nd_melt.country_j.map(countries.set_index('Japanese').Country.to_dict())
nd_melt['dev_j'] = nd_melt.country_j.map(countries.set_index('Japanese').Dev_j.to_dict())
nd_melt['region_j'] = nd_melt.country_j.map(countries.set_index('Japanese').Region_j.to_dict())


# In[154]:


nd_melt


# In[155]:


pd.merge(nc_melt, pd.melt(new_cases_per_100k.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases_1k'),
         on=['date','country_j'], how='outer').to_csv('/Users/anna/Documents/covid19/data-studio/data/for_gds/new_cases_7dma.csv', encoding='utf_8_sig')


# In[156]:


pd.merge(nd_melt, pd.melt(new_deaths_per_100k.rename_axis('date').reset_index(),id_vars='date',var_name='country_j',value_name='cases_1k'),
         on=['date','country_j'], how='outer').to_csv('/Users/anna/Documents/covid19/data-studio/data/for_gds/new_deaths_7dma.csv', encoding='utf_8_sig')


# In[ ]:




