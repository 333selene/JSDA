#https://www.mof.go.jp/english/policy/jgbs/debt_management/guide.htm

from pathlib import Path
import requests
import csv
import re
from bs4 import BeautifulSoup
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
import json
from AutomationFunctions import getCodes
import pandas as pd
import argparse
import os
import pandas as pd
from xlrd import XLRDError
import subprocess
from time import sleep
import random
from tqdm import tqdm

def gen_dates(year,start_date,end_date,dates):  
    print(f'Getting dates for {year}.....')
    current_year=datetime.now().year
    _start_date=datetime.strptime(start_date,'%Y%m%d')
    #print(_start_date)
    _end_date=datetime.strptime(end_date,'%Y%m%d')
    #print(_end_date)
    if year ==current_year:
        u='https://market.jsda.or.jp/en/statistics/bonds/prices/otc/index.html'
        r=requests.get(u)
        s=BeautifulSoup(r.content,'html.parser')
        table=s.find_all('td',attrs={'valign':'middle'})
        for i in range(0,len(table)+1,4):
            try:
                newdate=table[i].text
                if datetime.strptime(newdate,'%Y.%m.%d') < _start_date:
                    continue
                elif datetime.strptime(newdate,'%Y.%m.%d') > _end_date:
                    continue
                else:
                    if newdate not in dates:
                        dates.append(newdate)
            except IndexError:
                break
    u=f'https://market.jsda.or.jp/en/statistics/bonds/prices/otc/archive{year}.html'
    r=requests.get(u)
    s=BeautifulSoup(r.content,'html.parser')
    table=s.find_all('td',attrs={'valign':'middle'})
    for item in table:
        if item.text not in [' ','-']:
            newdate=item.text
            if datetime.strptime(newdate,'%Y.%m.%d') < _start_date:
                continue
            elif datetime.strptime(newdate,'%Y.%m.%d') > _end_date:
                continue
            else:
                if newdate not in dates:
                    dates.append(item.text)   

def get_csvs(date):
    #for date in dates:
    sleep(random.uniform(0.1,0.4))
    d=date.split('.')
    YYYY=d[0]
    y=YYYY[2:]
    m=d[1]
    if len(m)==1:m='0'+m
    day=d[2]
    if len(day)==1:day='0'+day
    d=dt.datetime.strptime(date,'%Y.%m.%d')
    date2=dt.datetime.strptime('2016.01.01','%Y.%m.%d')
    if d>=date2: 
        E='ES'
        root='en/statistics/bonds/prices/otc/files'
    else: 
        E='S'
        root='shijyo/saiken/baibai/baisanchi/files'
    u=f'https://market.jsda.or.jp/{root}/{YYYY}/{m}/{E}{y}{m}{day}.csv'
    if int(YYYY) >=2020:
        u=f'https://market.jsda.or.jp/{root}/{YYYY}/ES{y}{m}{day}.csv'
    r=requests.get(u)
    if r.status_code != 200:
        print(r.status_code)
        print(u)
        print(date)
        print("Error Downloading CSV for:",str(date), "Error Code:",str(r.status_code))
    else:
        # print("Download successful: ",date)
        filename=f'downloads/{YYYY}{m}{day}.csv'
        with open(filename,'wb') as file:
            file.write(r.content)

def sortDatesByYear(dates):
    datesByYear={}
    for date in dates:
        if date.replace(' ','')=='':
            dates.remove(date)
            continue
        d=date.split('.')
        y=d[0]
        m=d[1]
        if len(m)==1:m='0'+m
        day=d[2]
        if len(day)==1:day='0'+day
        d=y+m+day
        if y in datesByYear.keys():
            datesByYear[y].append(d)
        else:
            datesByYear[y]=[d]
    return datesByYear

def assemble_csvs(year,datesByYear):
    header=["Date","Type","Code","Issue","Due Date","Coupon Rate","Average Compound Yield","Average Price(Yen)","Change(0.01Yen)","Interest Payment Month","Interest Payment Day","~","~","~","Average Simple Yield","High Price (Yen)","High Simple Yield ","Low Price(Yen)","Low Simple Yield","~","No of Reporting Members","Highest Compound Yield","Highest Price Change(0.01 Yen)","Lowest Compound Yield","Lowest Price Change(0.01 Yen)","Median Compound Yield","Median Simple Yield","Median Price(Yen)","Median Price Change(0.01 Yen)"]
    dfs=pd.DataFrame(columns=header)
    for date in datesByYear[year]:
        #print(date)
        file=f'downloads/{date}.csv'
        try:
            df=pd.read_csv(file, encoding="shift-jis",header=None)
        except UnicodeDecodeError:
            df=pd.read_csv(file, encoding='utf-8',header=None)
        df.columns=header
        df = df[df['Type'].isin([1,2,5])]
        dfs=pd.concat([dfs,df],ignore_index=True)
    dfs.to_csv(f'compiled/{year}.csv',index=False,encoding='shift-jis')
    print('Saved compiled/'+str(year)+'.csv')

def assemble_master_csv(start_year,end_year): 
    header=["Date","Type","Code","Issue","Due Date","Coupon Rate","Average Compound Yield","Average Price(Yen)","Change(0.01Yen)","Interest Payment Month","Interest Payment Day","~","~","~","Average Simple Yield","High Price (Yen)","High Simple Yield ","Low Price(Yen)","Low Simple Yield","~","No of Reporting Members","Highest Compound Yield","Highest Price Change(0.01 Yen)","Lowest Compound Yield","Lowest Price Change(0.01 Yen)","Median Compound Yield","Median Simple Yield","Median Price(Yen)","Median Price Change(0.01 Yen)"]
    dfs=pd.DataFrame(columns=header)
    for year in range(start_year,end_year+1):
        #print(year)
        file=f'compiled/{year}.csv'
        try:
            df=pd.read_csv(file,encoding='shift-jis')
        except UnicodeDecodeError:
            df=pd.read_csv(file, encoding='utf-8')
        df.columns=header
        dfs=pd.concat([dfs,df],ignore_index=True)
    dfs.to_csv(f'jsda/ALL.csv',index=False,encoding='shift-jis')
    print('Saved jsda/all.csv')

def download_JBS():
    old_codes={
        '3-month': 'O',
        '6-month': 'H',
        '1-year': 'R'
    }
    pre_2009_dict={}
    if not os.path.exists('downloads/JGBs.xls'):
        u='https://www.mof.go.jp/english/policy/jgbs/auction/past_auction_results/Auction_Results_for_JGBs.xls'
        r=requests.get(u)
        with open('downloads/JGBs.xls','wb') as file:
            file.write(r.content)   
    if not os.path.exists('files/pre2009.json'):
        df=pd.read_excel('downloads/JGBs.xls',skiprows=[1,2],sheet_name='TB ')
        df=df.iloc[:, :2]
        a=df[df.columns[0]].tolist()
        b=df[df.columns[1]].tolist()
        for i in range(len(a)):
            b[i]=old_codes[b[i]]
            pre_2009_dict[a[i]] = [b[i]]
        with open(f'files/pre2009.json','w') as json_file:
            json.dump(pre_2009_dict,json_file)

def download_TBILLS(year,post_2009_dict):
    new_codes={
    '2-month': 'B',
    '3-month': '3',
    '6-month': '6',
    '1-year': 'E'
    }
    if not os.path.exists('downloads/TBILLS.xls'):            
        u='https://www.mof.go.jp/english/policy/jgbs/auction/past_auction_results/Auction_Results_for_T-bills.xls'
        r=requests.get(u)
        with open('downloads/TBILLS.xls','wb') as file:
            file.write(r.content)
    try:
        df=pd.read_excel('downloads/TBILLS.xls',skiprows=[1,2],sheet_name=f'FY{year}')
    except XLRDError:
        try:
            
            df=pd.read_excel('downloads/TBILLS.xls',skiprows=[1,2],sheet_name=f'FY{year} ')    
        except XLRDError:
            print(f'No sheet named <\'FY{year} \'> in TBILLS tenor master file. Checking auction website for tbill tenors...')    
            return None
    df=df.iloc[:, :2]
    a=df[df.columns[0]].tolist()
    b=df[df.columns[1]].tolist()
    dictionary={}
    for i in range(len(a)):
        b[i]=new_codes[b[i]]
        dictionary[a[i]] = [b[i]]
    with open(f'files/{year}tbills.json','w') as json_file:
        json.dump(dictionary,json_file)
    with open(f'files/{year}tbills.json','r') as file:
        j=json.load(file)
        for key in j.keys():
            post_2009_dict[key]=j[key]

def tenorsFromAuctionPage(url,post_2009_dict):
    new_codes={
    '2-month': 'B',
    '3-month': '3',
    '6-month': '6',
    '1-year': 'E'
    }
    r=requests.get(url)
    soup=BeautifulSoup(r.content,'html.parser')
    t=soup.find_all('td',class_='sente')
    for a in t:
        a=a.text.split('(')
        if a[0]=='Treasury Discount Bills ' and len(a)>2:
            tenor=a[1][:-1]
            tenor=new_codes[tenor]
            issueNum=a[2][:-1]
            post_2009_dict[issueNum]=[tenor]

def get_TBILL_tenors(start_year,end_year):
    current_year=datetime.now().year
    current_month=datetime.now().month
    _start_year=int(start_year)-2  #go back 2 years to ensure we get every issue
    
    if os.path.exists('downloads/TBILLS.xls'):
        os.remove('downloads/TBILLS.xls')
    if os.path.exists('downloads/JGBs.xls'):
        os.remove('downloads/JGBs.xls')
    
    post_2009_dict={}
    
    for year in range(_start_year,end_year+1):
        # print(year)
        if year < 2008:
            download_JBS()       
        if year==2008 or year==2009:
            download_JBS()
            download_TBILLS(year,post_2009_dict)
        if year >2009:
            download_TBILLS(year,post_2009_dict)        
            if year == current_year and current_month!=1:
                prev_month=(datetime.now()-relativedelta(months=1)).month
                current_month='0'+str(datetime.now().month)
                current_month=current_month[-2:]
                prev_month='0'+str(prev_month)
                prev_month=current_month[-2:]
                u1=f'https://www.mof.go.jp/english/policy/jgbs/auction/calendar/{current_year}{prev_month}e.htm'
                u2=f'https://www.mof.go.jp/english/policy/jgbs/auction/calendar/{current_year}{current_month}e.htm'
                print("Getting TBILL tenors for previous month at:",u1)
                print("Getting TBILL tenors for current month at:",u2)
                tenorsFromAuctionPage(u1,post_2009_dict)                
                tenorsFromAuctionPage(u2,post_2009_dict)                
            if year == current_year and current_month==1:
                current_month='01'
                prev_month='12'
                current_year=str(year)[2:]
                prev_year=str(year-1)[2:]
                u1=f'https://www.mof.go.jp/english/policy/jgbs/auction/calendar/{prev_year}{prev_month}e.htm'
                u2=f'https://www.mof.go.jp/english/policy/jgbs/auction/calendar/{current_year}{current_month}e.htm'
                print("Getting TBILL tenors for previous month at:",u1)
                print("Getting TBILL tenors for current month at:",u2)
                tenorsFromAuctionPage(u1,post_2009_dict)
                tenorsFromAuctionPage(u2,post_2009_dict)
    if post_2009_dict!={}:    
        with open(f'files/post2009.json','w') as json_file:
            json.dump(post_2009_dict,json_file)                

class Security:
    #UTILITIES
    pattern = re.compile(r'[\d ()\\/）　]')
    def ints_only(text)->int:
        ints=re.findall(r'\d+',text)
        output=''.join(ints)
        return output
    
    def forceFourDigit(x):
        #print('forceFour input:' +x)
        assert len(str(x))<5
        x='000'+str(x)
        x=x[-4:]
        #print('forceFour output:' +x)
        return x
    ###
    #DICTIONARIES
    jp_dict={
        '超長期国債': 'JGBEXTENDED',
        '物価連動国債': 'JGB IL JP',
        '長期国債': 'JGBLONGTERM',
        '中期国債':'JGBMIDTERM',
        '国庫短期証券':'T-BILL',   
        '変利国債':'JGBFR',
        '短期国債':'T-BILL OLD'
    }
    def translator(self,input_text,identifyParseType=False):
        if identifyParseType==True:
            return Security.jp_dict[input_text]
        else:
            #print('translator input '+ input_text)
            #print('translator output '+input_text.replace(self.issueCleaned,Security.jp_dict[self.issueCleaned]))
            return input_text.replace(self.issueCleaned,Security.jp_dict[self.issueCleaned])
    jgb_tenors_dict={
        '超長期国債': '20', #without tenor specified, assume 20 year.
        '長期国債':'10',  #without tenor specified, assume 10 year, etc.
        '中期国債':'5',
    }
    jgb_dict={
        '2':'2',
        '4':'F',
        '5':'5',
        '6':'X',
        '10':'A',
        '20':'K',
        '30':'T',
        '40':'Y'
    }
    jgbFR_dict={
        '15':'L',
    }
    split_duration_dict={
        'A':'10',
        'K':'20',
        'T':'30',
        'Y':'40',
        'L':'15',
        'I':'10'
    }
    #jgbIL_dict={}
    ###
    def __init__(self, issue):
        self.issue=issue
        self.issueCleaned=Security.pattern.sub('',issue)
        
        if '(' in issue: self.tenorSpecified=True
        else: self.tenorSpecified=False

        if self.issueCleaned in Security.jp_dict.keys():
            self.parseType=Security.translator(self,self.issueCleaned,identifyParseType=True)
        else:
            self.parseType=self.issueCleaned
            
    def parseSecurity(self): 
        self.notMatched=False #default
        # JGB437(2)
        if self.parseType=='JGB':
            issue=self.issue
            if self.tenorSpecified==True:
                tenor=issue.split('(')[1].replace(')','')
            else:
                # print('NO TENOR FOR '+self.issue)
                exit
            self.tenor=str(int(tenor))
            self.secondLetter=Security.jgb_dict[tenor]
            issue=issue.split('(')[0]
            self.issueNumber=Security.forceFourDigit(Security.ints_only(issue))
            self.issueLabel = f'{self.tenor}-Year JGB{str(Security.ints_only(issue))}'
            self.notMatched=False
        # JGB I/L18
        elif self.parseType=='JGBIL':
            issue=self.issue
            self.secondLetter='I'
            self.issueNumber=Security.forceFourDigit('L'+str(Security.ints_only(issue)))
            self.issueLabel = f'JGB I/L{str(Security.ints_only(issue))}'   
            self.notMatched=False
        # 物価連動国債 13
        elif self.parseType=='JGB IL JP':
            issue=self.issue
            self.secondLetter='I'
            issueTranslated=Security.translator(self,self.issue)
            self.issueNumber=Security.forceFourDigit('L'+str(Security.ints_only(issue)))
            self.issueLabel = f'JGB I/L{str(Security.ints_only(issue))}' 
            self.notMatched=False
        # JGBFR 48(15)
        elif self.parseType=='JGBFR':
            issue=self.issue
            self.tenor = '15'
            self.secondLetter='L'
            issue=issue.split('(')[0]
            self.issueNumber=Security.forceFourDigit(Security.ints_only(issue))
            self.issueLabel = f'{self.tenor}-Year JGBFR {Security.ints_only(issue)}'
            self.notMatched=False
        #超長期国債(30)10 OR 超長期国債 5 
        elif self.parseType=='JGBEXTENDED':
            issue=self.issue
            if self.tenorSpecified:
                issueNumber=Security.ints_only(issue.split(')')[1])
                self.tenor=Security.ints_only(issue.split(')')[0].split('(')[1])
                self.secondLetter=Security.jgb_dict[self.tenor]
            else:
                issueNumber=Security.ints_only(Security.translator(self,self.issue))
                self.tenor=Security.jgb_tenors_dict[self.issueCleaned]
                self.secondLetter=Security.jgb_dict[self.tenor]
            self.issueNumber=Security.forceFourDigit(issueNumber)
            self.issueLabel = f'{self.tenor}-Year JGB{str(issueNumber)}'
            self.notMatched=False
        #長期国債 248  OR 長期国債 30(6)  
        elif self.parseType=='JGBLONGTERM':
            issue=self.issue
            if self.tenorSpecified:
                issueNumber=Security.ints_only(issue.split('(')[0])
                self.tenor=Security.ints_only(issue.split('(')[1])
                self.secondLetter=Security.jgb_dict[self.tenor]
            else:
                issueTranslated=Security.translator(self,self.issue)
                issueNumber=Security.ints_only(issueTranslated)
                self.tenor=Security.jgb_tenors_dict[self.issueCleaned]
                self.secondLetter=Security.jgb_dict[self.tenor]
            self.issueNumber=Security.forceFourDigit(issueNumber)
            self.issueLabel = f'{self.tenor}-Year JGB{str(issueNumber)}'
            self.notMatched=False
        #中期国債 56(5)  
        elif self.parseType=='JGBMIDTERM':
            issue=self.issue
            if self.tenorSpecified:
                issueNumber=Security.ints_only(issue.split('(')[0])
                self.tenor=Security.ints_only(issue.split('(')[1])
                self.secondLetter=Security.jgb_dict[self.tenor]
            else:
                issueTranslated=Security.translator(self,self.issue)
                issueNumber=Security.ints_only(issueTranslated)
                self.tenor=Security.jgb_tenors_dict[self.issueCleaned]
                self.secondLetter=Security.jgb_dict[self.tenor]
            self.issueNumber=Security.forceFourDigit(issueNumber)
            self.issueLabel = f'{self.tenor}-Year JGB{issueNumber}'
            self.notMatched=False
        # 短期国債 439
        elif self.parseType=='T-BILL OLD':
            issue=self.issue
            issueNumber=Security.ints_only(issue)
            with open('files/pre2009.json','r') as file:
                j=json.load(file)
                self.secondLetter=j[issueNumber][0]
                self.issueNumber=Security.forceFourDigit(issueNumber)
            self.issueLabel = f'T-BILL{issueNumber} pre-'
            self.notMatched=False
        # T-BILL 326
        elif self.parseType=='T-BILL':
            issue=self.issue
            issueNumber=Security.ints_only(issue)
            with open('files/post2009.json','r') as file:
                j=json.load(file)
                if issueNumber not in j.keys():
                    new_codes={
                    '2': 'B',
                    '3': '3',
                    '6': '6',
                    'Y': 'E'
                    }
                    print('ERROR, TBILL TENOR NOT FOUND, MANUAL INPUT REQUIRED:  ',issueNumber)
                    manualTenor=str(input(f"Find the tenor of TBILL NUMBER {issueNumber} AND TYPE 2, 3, 6, or Y"))
                    while manualTenor not in new_codes.keys():
                        manualTenor=str(input(f"Find the tenor of TBILL NUMBER {issueNumber} AND TYPE 2, 3, 6, or Y"))
                    self.secondLetter=new_codes[manualTenor]
                else:
                    self.secondLetter=j[issueNumber][0]
            self.issueNumber=Security.forceFourDigit(issueNumber)
            
            self.issueLabel = f'T-BILL{issueNumber} ' 
            self.notMatched=False
        else:
            # print(self.issue,'NOT MATCHED')
            self.notMatched=True

        if self.parseType=='JGBWI' or self.parseType=='JGBWI-':
            self.isWI=True
        else: self.isWI=False
        
    def genCodes(self,dueDatesByIssue,dueDatesByCode,securityIdsByIssue,securityIdsByCode,issueLabelsByCode):
        self.dueDate=dueDatesByIssue[self.issue]
        self.securityId=securityIdsByIssue[self.issue]
        if self.notMatched==False:
            letters=['P','C','S','R','O','I','H','M','E','L','U','Y','N']
            letters_if_tbill_or_IL=['P','S','R','I','H','E','L','Y','N']
            series=[]
            if self.parseType in ['T-BILL', 'T-BILL OLD', 'JGBIL', 'JGB IL JP']:
                for letter in letters_if_tbill_or_IL:
                    series.append('J'+self.secondLetter+self.issueNumber+letter)
            else:
                for letter in letters:
                    series.append('J'+self.secondLetter+self.issueNumber+letter)
            self.series=series
            for s in self.series: 
                dueDatesByCode.update({s:self.dueDate})
                securityIdsByCode.update({s:self.securityId})
                issueLabelsByCode.update({s:self.issueLabel})
            #print(series)

def identifyMatureIssues(start_year,end_year,today_str): 
    today=int(today_str)
    mature_issues=[]
    for year in range(start_year,end_year+1):
        df=pd.read_csv(f'compiled/{year}.csv',encoding="shift-jis")
        df = df[df['Due Date'] <= today]
        # print(df)
        mature_issues.extend((df['Issue'].tolist()))
    mature_issues=set(mature_issues)
    return mature_issues

def getAllDueDates(start_year, end_year):
    dueDatesByIssue={}
    for year in range(start_year,end_year+1):
        df=pd.read_csv(f'compiled/{year}.csv',encoding='shift-jis')
        df[['Due Date','Issue']]
        df_dict = df.set_index('Issue')['Due Date'].to_dict()
        dueDatesByIssue.update(df_dict)    
    return dueDatesByIssue

def getAllSecurityIds(start_year, end_year):
    securityIdsByIssue={}
    for year in range(start_year,end_year+1):
        df=pd.read_csv(f'compiled/{year}.csv',encoding='shift-jis')
        df[['Code','Issue']]
        df_dict = df.set_index('Issue')['Code'].to_dict()
        securityIdsByIssue.update(df_dict)    
    return securityIdsByIssue

def compileCodes(year,mature_issues,dueDatesByIssue,dueDatesByCode,securityIdsByIssue,securityIdsByCode,issueLabelsByCode):
    needSplitCodes=['A','K','T','Y','L','I']
    df=pd.read_csv(f'compiled/{year}.csv',encoding='shift-jis')
    issues=df['Issue'].tolist()      
    detected={}
    mature=[]
    detected_splits={}
    mature_splits=[]
    # print(set(issues))
    for i in set(issues):
        matureIssue=False
        if i in mature_issues:
            matureIssue=True
            print('found mature issue: ', i)
        #print('Processing:',i)
        s=Security(i)
        s.parseSecurity()
        not_matched=[]
        if s.notMatched:
            not_matched.append(f"{i} '-' {s.parseType}")
        s.genCodes(dueDatesByIssue,dueDatesByCode,securityIdsByIssue,securityIdsByCode,issueLabelsByCode)  #generates codes and translates issues to codes
        if s.notMatched==False:
            for code in s.series:  
                if code[1] in needSplitCodes:
                    if code not in detected_splits.keys():
                        detected_splits[code]=i
                    if matureIssue:
                        mature_splits.append(code)
                else:
                    if code not in detected.keys():
                        detected[code]=i           
                    if matureIssue:
                        mature.append(code) 
    # if this code makes no sense j text me 9175958390 lol 
    return detected,mature,not_matched,detected_splits,mature_splits

def sortSplits(detected_splits,series_on_network):
    new_split_series=set()
    existing_split_series=set()
    for code in detected_splits.keys():
        if code in series_on_network:
            existing_split_series.add(code)
        else:
            new_split_series.add(code)
    return new_split_series,existing_split_series

def getSplitYears(series_list,dueDatesByCode):
    splitYears={}
    today_year=datetime.today().year
    for code in series_list:
        due_date=str(dueDatesByCode[code])
        due_year=datetime.strptime(due_date,"%Y%m%d").year
        # if due_year>today_year:
        #     due_year=today_year
        if due_year in splitYears.keys():
            splitYears[due_year].append(code)
        else:
            splitYears[due_year]=[code]
    return splitYears

def genSplitCodes(splitYears):
    split_codes_with_trailing_numbers=set()
    _years=[]
    tenors=Security.split_duration_dict
    for endYear in splitYears.keys():
        for code in splitYears[endYear]:
            split_codes_with_trailing_numbers.add(code)
            series_start_year=int(endYear)-int(tenors[code[1]])
            for x in range(int(endYear), 2001, -8):
                _years.append(x)
                if len(_years)>1:
                    secondSplit=_years[1]
                    if secondSplit >= series_start_year:
                        code2=code+'2'
                        split_codes_with_trailing_numbers.add(code2) 
                if len(_years)>2:
                    thirdSplit=_years[2]
                    if thirdSplit >= series_start_year-8:
                        code3=code+'3'
                        split_codes_with_trailing_numbers.add(code3)
                #fourthSplit=_years[3] 
    return split_codes_with_trailing_numbers

def setSplitCodeMaxYearToEndYear(splitYears,end_year):
    _del=[]
    for year in splitYears.keys():
        if year > end_year:
            splitYears[end_year].extend(splitYears[year])
            _del.append(year)
    for year in _del:
        del splitYears[year]

def codesOnNetwork():
    ll=getCodes("J74","INTDAILY")
    return ll

class GenFiles:
    dict_header="MATCH_HEADING ||| NONE \nDATES ||| YYYYMMDD ||| \nDELIMITER ||| , \nTRANS ||| - ||| \n"  
    ## LABELS ##
    label_base="Japan: "
    labels_security_type={
    "2": "JGB(2): ",
    "5": "JGB(5) ",
    "A": "JGB(10) ",
    "K": "JGB(20) ",
    "T": "JGB(30) ",
    "Y": "JGB(40) ",
    "I": "JGB I/L: ",
    "F": "JGB(4): ",
    "X": "JGB(6): ",
    "L": "JGB(15)FR: ",
    "3": "T-Bills: 3-Month: ",
    "6": "T-Bills: 6-Month: ",
    "E": "T-Bills: 1-Year: ",
    "B": "T-Bills: 2-Month: ",
    "O": "T-Bills: 3-Month OLD: ",
    "H": "T-Bills: 6-Month OLD: ",
    "R": "T-Bills: 1-Year OLD: ",
    }
    labels_series={
    "C": "Average Comp Yield",
    "P": "Average Price",
    "S": "Average Simple Yield",
    "H": "High Price",
    "E": "High Simple Yield ",
    "L": "Low Price",
    "Y": "Low Simple Yield",
    "N": "Reporting Members",
    "M": "High Comp Yield",
    "U": "Low Comp Yield",
    "O": "Median Comp Yield",
    "I": "Median Simple Yield",
    "R": "Median Price"
    }
    labels_units={
    "C": "(%)",
    "P": "(% of Par)",
    "S": "(%)",
    "H": "(% of Par)",
    "E": "(%)",
    "L": "(% of Par)",
    "Y": "(%)",
    "N": "(No.)",
    "M": "(%)",
    "U": "(%)",
    "O": "(%)",
    "I": "(%)",
    "R": "(% of Par)"
    }
    dict_dp={
    "C": "/// *1 /// 3  ",
    "P": "/// *1 /// 2  ",
    "S": "/// *1 /// 3  ",
    "H": "/// *1 /// 2  ",
    "E": "/// *1 /// 3  ",
    "L": "/// *1 /// 2  ",
    "Y": "/// *1 /// 3  ",
    "N": "/// *1 /// 0  ",
    "M": "/// *1 /// 3  ",
    "U": "/// *1 /// 3  ",
    "O": "/// *1 /// 3  ",
    "I": "/// *1 /// 3  ",
    "R": "/// *1 /// 2  "
    }
    dict_headers={
    "C": "Average Compound Yield",
    "P": "Average Price(Yen)",
    "S": "Average Simple Yield",
    "H": "High Price (Yen)",
    "E": "High Simple Yield ",
    "L": "Low Price(Yen)",
    "Y": "Low Simple Yield",
    "N": "No of Reporting Members",
    "M": "Highest Compound Yield",
    "U": "Lowest Compound Yield",
    "O": "Median Compound Yield",
    "I": "Median Simple Yield",
    "R": "Median Price(Yen)",
    }
    trans_2015={
        
    }
    par_dif={
    "C": "1",
    "P": "0",
    "S": "1",
    "H": "0",
    "E": "1",
    "L": "0",
    "Y": "1",
    "N": "0",
    "M": "1",
    "U": "1",
    "O": "1",
    "I": "1",
    "R": "0"
    }
    par_agg={
    "C": "1",
    "P": "3",
    "S": "1",
    "H": "3",
    "E": "1",
    "L": "3",
    "Y": "1",
    "N": "3",
    "M": "1",
    "U": "3",
    "O": "1",
    "I": "1",
    "R": "3"
    }
    par_grp="J74"
    par_geo="158"
    par_mag={
    "C": "0",
    "P": "0",
    "S": "0",
    "H": "0",
    "E": "0",
    "L": "0",
    "Y": "0",
    "N": "0",
    "M": "0",
    "U": "0",
    "O": "0",
    "I": "0",
    "R": "0"
    }
    par_typ={
    "C": "%",
    "P": "LocCur",
    "S": "%",
    "H": "LocCur",
    "E": "%",
    "L": "LocCur",
    "Y": "%",
    "N": "Units",
    "M": "%",
    "U": "%",
    "O": "%",
    "I": "%",
    "R": "LocCur"
    }
    
    def __init__(self,code,issue,DISC=False,dueDatesByCode=None,securityIdsByCode=None,issueLabelsByCode=None):
        self.code=code
        if type(issue) is list:
            issue=issue[0]
        self.issue=issue
        self.secondLetter=self.code[1]
        self.issueNum=str(int(self.code[2:6].replace('L','')))
        self.codeType=self.code[0:7][-1]
        self.seriesType=GenFiles.labels_series[self.codeType]
        self.labelUnits=GenFiles.labels_units[self.codeType]
        if dueDatesByCode:
            dueDate=str(dueDatesByCode[self.code[0:7]])
            _dueDate=datetime.strptime(dueDate,'%Y%m%d')
            self.dueDate=datetime.strftime(_dueDate,'%m/%d/%Y')
        if securityIdsByCode:
            self.securityId=str(securityIdsByCode[self.code[0:7]])
        if issueLabelsByCode:
            self.issueLabel=issueLabelsByCode[self.code[0:7]]
        self.disc=""
        if DISC==True:
            self.disc="[MAT]"
    
    def genDictionaryEntry(self):        
        self.dictEntry=f"{self.code[0:7]} {GenFiles.dict_dp[self.codeType]} ||| {self.issue} ||| {GenFiles.dict_headers[self.codeType]}\n"
        #print(self.dictEntry)
    def genLabel(self):
        self.label=f'@DES {self.code}\n{GenFiles.label_base}{self.issueLabel}[{self.securityId}] due {self.dueDate}: {self.seriesType}{self.disc} {self.labelUnits}\n'
        #Germany: 0.200 BSA 22 [isn] due 6/14/2024: Price (% of Par)
    def genPar(self):
        self.param=f"@PAR {self.code}  MAG={GenFiles.par_mag[self.codeType]}  AGG={GenFiles.par_agg[self.codeType]}  DIF={GenFiles.par_dif[self.codeType]}  TYP={GenFiles.par_typ[self.codeType]}  GEO={GenFiles.par_geo}  GRP={GenFiles.par_grp}\n"

def checkDisc(disc_series,series_on_network):
    with open('jsda/check_disc.lst','w',newline='') as file:   
        for series in disc_series:
            if series in series_on_network:
                file.write(series+"\n")
    os.chdir("jsda")
    subprocess.call("mirror",shell=True)
    subprocess.run("check_disc.bat")
    os.chdir("..")
    disc_series_verified=[]
    if os.path.exists('jsda\\check_labels.lst'):
        with open('jsda\\check_labels.lst', 'r') as file:
            for line in file:
                line=line.replace('\n','')
                if line[0] == "@":
                    code = line.split(" ")[1]
                else:
                    if "[MAT]" not in line:                        
                        disc_series_verified.append(code)
    return disc_series_verified
        
def genLabels2(series,dueDatesByCode,securityIdsByCode,issueLabelsByCode,today_str):
    labels=[]
    for code in series:
        disc=False
        dueDate=dueDatesByCode[code[0:7]]
        if int(dueDate) <= int(today_str):
            disc=True
        securityId=securityIdsByCode[code[0:7]]
        issueLabel=issueLabelsByCode[code[0:7]]
        s1=GenFiles(code[0:7],None,DISC=disc,dueDatesByCode=dueDatesByCode,securityIdsByCode=securityIdsByCode,issueLabelsByCode=issueLabelsByCode)
        s1.genLabel()
        label=s1.label
        s2=GenFiles(code,None,DISC=disc,dueDatesByCode=dueDatesByCode,securityIdsByCode=securityIdsByCode,issueLabelsByCode=issueLabelsByCode)
        s2.genLabel()
        label2=s2.label
        if label not in labels:
            labels.append(label)
        if label2 not in labels:
            labels.append(label2)
    return labels

def writeLabels(new_labels,disc_labels):
    if len(new_labels)>0:
        with open('add/new.lab','w') as file:
            for label in new_labels:
                file.write(label)
    if len(disc_labels)>0:
        with open('jsda/disc.lab','w') as file:
            for label in disc_labels:
                file.write(label) 
            
def genNewPar(new_series):
    allParams=[]
    for code in new_series:
        s1=GenFiles(code,None)
        s1.genPar()
        param=s1.param
        if param not in allParams:
            allParams.append(param)
    with open('add/new.par','w') as file:
        file.writelines(allParams)

def genJsdaYearDict(detected_series_existing_by_year,detected_series_row_labels,detected_series_row_labels_jp):
    for year in detected_series_existing_by_year.keys():
        jsdaDict=[]
        jsdaDict.append(GenFiles.dict_header)
        if int(year) <=2015:
            for code in detected_series_existing_by_year[year]:
                s1=GenFiles(code,detected_series_row_labels_jp[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in jsdaDict:
                    jsdaDict.append(dictEntry)
            with open(f'jsda/{year}.dic','w',encoding='shift-jis') as file:
                file.writelines(jsdaDict)
        else:
            for code in detected_series_existing_by_year[year]:
                s1=GenFiles(code,detected_series_row_labels[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in jsdaDict:
                    jsdaDict.append(dictEntry)
            with open(f'jsda/{year}.dic','w',encoding='shift-jis') as file:
                file.writelines(jsdaDict)
    
def genJsdaDict(detected_series_existing,detected_series,year,Split=False):    
    jsdaDict=[]
    jsdaDict.append(GenFiles.dict_header)
    for code in detected_series_existing:
        #print(code)
        s1=GenFiles(code,detected_series[code])
        s1.genDictionaryEntry()
        dictEntry=s1.dictEntry
        if dictEntry not in jsdaDict:
            jsdaDict.append(dictEntry)
    #print(jsdaDict)
    if Split==False:
        with open(f'jsda/{year}.dic','w',encoding='shift-jis') as file:
            file.writelines(jsdaDict)
    else:
        with open(f'jsda/{year}split.dic','w',encoding='shift-jis') as file:
            file.writelines(jsdaDict)

def genJsdaYearSplitDict(split_series_by_due_year,detected_split_series_row_labels,detected_split_series_row_labels_jp):
    for year in split_series_by_due_year.keys():
        newDict=[]
        newJpDict=[]
        newDict.append(GenFiles.dict_header)
        newJpDict.append(GenFiles.dict_header)
        if int(year) <= 2015:
            for code in split_series_by_due_year[year]:
                s2=GenFiles(code,detected_split_series_row_labels_jp[code])
                s2.genDictionaryEntry()
                dictEntry=s2.dictEntry
                if dictEntry not in newJpDict:
                    newJpDict.append(dictEntry)
            if len(newJpDict)>1:
                with open(f'jsda/{year}split_JP.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newJpDict:
                        file.write(line)  
        else:
            for code in split_series_by_due_year[year]:
                s1=GenFiles(code,detected_split_series_row_labels[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in newDict:
                    newDict.append(dictEntry)
                if code in detected_split_series_row_labels_jp.keys():
                    s2=GenFiles(code,detected_split_series_row_labels[code])
                    s2.genDictionaryEntry()
                    dictEntry=s2.dictEntry
                    if dictEntry not in newJpDict:
                        newJpDict.append(dictEntry)             
            if len(newDict)>1:
                with open(f'jsda/{year}split.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newDict:
                        file.write(line)      
            if len(newJpDict)>1:
                with open(f'jsda/{year}split_JP.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newJpDict:
                        file.write(line)  

def genJsdaYearSplitDict_n(split_series_by_due_year,detected_split_series_row_labels,detected_split_series_row_labels_jp):
    for year in split_series_by_due_year.keys():
        newDict=[]
        newJpDict=[]
        newDict.append(GenFiles.dict_header)
        newJpDict.append(GenFiles.dict_header)
        if int(year) <= 2015:
            for code in split_series_by_due_year[year]:
                s2=GenFiles(code,detected_split_series_row_labels_jp[code])
                s2.genDictionaryEntry()
                dictEntry=s2.dictEntry
                if dictEntry not in newJpDict:
                    newJpDict.append(dictEntry)
            if len(newJpDict)>1:
                with open(f'jsda/{year}split_JP_n.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newJpDict:
                        file.write(line)  
        else:
            for code in split_series_by_due_year[year]:
                s1=GenFiles(code,detected_split_series_row_labels[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in newDict:
                    newDict.append(dictEntry)
                if code in detected_split_series_row_labels_jp.keys():
                    s2=GenFiles(code,detected_split_series_row_labels[code])
                    s2.genDictionaryEntry()
                    dictEntry=s2.dictEntry
                    if dictEntry not in newJpDict:
                        newJpDict.append(dictEntry)             
            if len(newDict)>1:
                with open(f'jsda/{year}split_n.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newDict:
                        file.write(line)      
            if len(newJpDict)>1:
                with open(f'jsda/{year}split_JP_n.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newJpDict:
                        file.write(line) 
        
def genAddYearDict(new_series_by_year,detected_series_row_labels,detected_series_row_labels_jp):
    for year in new_series_by_year.keys():
        newDict=[]
        newDict.append(GenFiles.dict_header)
        if int(year) <=2015:
            for code in new_series_by_year[year]:
                s1=GenFiles(code,detected_series_row_labels_jp[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in newDict:
                    newDict.append(dictEntry)
        else:
            for code in new_series_by_year[year]:
                s1=GenFiles(code,detected_series_row_labels[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in newDict:
                    newDict.append(dictEntry)
        with open(f'add/add{year}.dic','w',encoding='shift-jis',newline='') as file:
            for line in newDict:
                file.write(line)

def genAddYearSplitDict(new_split_series_by_year,detected_split_series_row_labels,detected_split_series_row_labels_jp):
    for year in new_split_series_by_year.keys():
        newDict=[]
        newJpDict=[]
        newDict.append(GenFiles.dict_header)
        newJpDict.append(GenFiles.dict_header)
        if int(year) <= 2015:
            for code in new_split_series_by_year[year]:
                s2=GenFiles(code,detected_split_series_row_labels_jp[code])
                s2.genDictionaryEntry()
                dictEntry=s2.dictEntry
                if dictEntry not in newJpDict:
                    newJpDict.append(dictEntry)
            if len(newJpDict)>1:
                with open(f'add/add{year}split_JP.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newJpDict:
                        file.write(line)  
        else:
            for code in new_split_series_by_year[year]:
                s1=GenFiles(code,detected_split_series_row_labels[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in newDict:
                    newDict.append(dictEntry)
                if code in detected_split_series_row_labels_jp.keys():
                    s2=GenFiles(code,detected_split_series_row_labels[code])
                    s2.genDictionaryEntry()
                    dictEntry=s2.dictEntry
                    if dictEntry not in newJpDict:
                        newJpDict.append(dictEntry)             
            if len(newDict)>1:
                with open(f'add/add{year}split.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newDict:
                        file.write(line)      
            if len(newJpDict)>1:
                with open(f'add/add{year}split_JP.dic','w',encoding='shift-jis',newline='') as file:
                    for line in newJpDict:
                        file.write(line)  
            
def genNifDict(new_series_by_year,detected_series_row_labels,detected_series_row_labels_jp):
    for year in new_series_by_year.keys():
        nifDict=[]
        nifDict.append(GenFiles.dict_header)
        if int(year) <=2015:
            for code in new_series_by_year[year]:
                s1=GenFiles(code,detected_series_row_labels_jp[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in nifDict:
                    nifDict.append(dictEntry)
        else:
            for code in new_series_by_year[year]:
                s1=GenFiles(code,detected_series_row_labels[code])
                s1.genDictionaryEntry()
                dictEntry=s1.dictEntry
                if dictEntry not in nifDict:
                    nifDict.append(dictEntry)
        with open(f'jsda/sub8YearNewSeries{year}.dic','w',encoding='shift-jis',newline='') as file:
            for line in nifDict:
                file.write(line)
                
def runAdd():
    addBatch="start cmd.exe @cmd /k f:/intdaily/japan/jsda/add/add.bat"
    subprocess.call(addBatch,shell=True)
    
def csv2modb(years):
    subprocess.call('del *.mod',shell=True)
    for year in years:
        csv2modb=f"csv2modb compiled/{year}.csv jsda/{year}.dic {year}.mod"
        subprocess.call(csv2modb,shell=True)
    subprocess.call('copy *.mod jsda.mod',shell=True)

def csv2modb_nif(year):
    csv2modb=f"csv2modb compiled/{year}.csv jsda/sub8YearNewSeries{year}.dic {year}sub8.mod"
    subprocess.call(csv2modb,shell=True)
    
def csv2modb_Split(year):
    csv2modb=f"csv2modb jsda/all.csv jsda/{year}split.dic {year}split_ENG.mod"
    csv2modb_jp=f"csv2modb jsda/all.csv jsda/{year}split_JP.dic {year}split_JP.mod"
    if 2009>=int(year):
        subprocess.call(csv2modb_jp,shell=True)
    if 2015>int(year)>2009:
        subprocess.call(csv2modb_jp,shell=True)
        subprocess.call(f"dwsplit {year}split.mod g:/util/{str(year)[2:]}daily.txt",shell=True)
        subprocess.call(f'del {year}split.mod',shell=True)
        subprocess.call(f'ren out.mod {year}split.mod',shell=True)
    if int(year) >=2015:
        subprocess.call(csv2modb_jp,shell=True)
        subprocess.call(csv2modb,shell=True)
        subprocess.call(f"copy {year}split_JP.mod+{year}split_ENG.mod {year}split.mod")
        subprocess.call(f"dwsplit {year}split.mod g:/util/{str(year)[2:]}daily.txt",shell=True)
        subprocess.call(f'del {year}split.mod',shell=True)
        subprocess.call(f'ren out.mod {year}split.mod',shell=True)
        
        
def csv2modb_Split_n(year):
    csv2modb=f"csv2modb jsda/all.csv jsda/{year}split_n.dic {year}split_ENG_n.mod"
    csv2modb_jp=f"csv2modb jsda/all.csv jsda/{year}split_JP_n.dic {year}split_JP_n.mod"
    if 2009>=int(year):
        subprocess.call(csv2modb_jp,shell=True)
    if 2015>int(year)>2009:
        subprocess.call(csv2modb_jp,shell=True)
        subprocess.call(f"dwsplit {year}split_n.mod g:/util/{str(year)[2:]}daily.txt",shell=True)
        subprocess.call(f'del {year}split_n.mod',shell=True)
        subprocess.call(f'ren out.mod {year}split_n.mod',shell=True)
    if int(year) >=2015:
        subprocess.call(csv2modb_jp,shell=True)
        subprocess.call(csv2modb,shell=True)
        subprocess.call(f"copy {year}split_JP_n.mod+{year}split_ENG_n.mod {year}split_n.mod")
        subprocess.call(f"dwsplit {year}split_n.mod g:/util/{str(year)[2:]}daily.txt",shell=True)
        subprocess.call(f'del {year}split_n.mod',shell=True)
        subprocess.call(f'ren out.mod {year}split_n.mod',shell=True)
        

def genAggList(detected_series_existing,new_series=None):
    agg_list=[]
    agg_codes=[]
    aggregated_series={
        'H':['','E'],
        'N':['','E']
    }
    for series in detected_series_existing:
        if series[-1] in aggregated_series.keys():
            for letter in aggregated_series[series[-1]]:
                agg_codes.append(series+letter)
                if letter == '':
                    agg_list.append(f'{series}  {series}    1\n')
                if letter == 'E':
                    agg_list.append(f'{series}  {series+letter}    3\n')
    subprocess.call('md w',shell=True)
    with open('w/1.lst','w') as file:
        file.writelines(agg_list)
    if new_series:
        new_list=[]
        new_agg_codes=[]
        for series in new_series:
            if series[-1] in aggregated_series.keys():
                for letter in aggregated_series[series[-1]]:
                    new_agg_codes.append(series+letter)
                    if letter == '':
                        new_list.append(f'{series}  {series}    1\n')
                    if letter == 'E':
                        new_list.append(f'{series}  {series+letter}    3\n')
        with open('add/add.lst','w') as file:
            file.writelines(new_list)
            print('Saved add.lst to ADD folder.')

def runDisc():
    disc_batch='start cmd.exe @cmd /k f:/intdaily/japan/jsda/jsda/disc.bat'
    subprocess.call(disc_batch,shell=True)
    

def stop_process_pool(executor):
    for pid, process in executor._processes.items():
        process.terminate()

def outputJsdaAud(dates,not_matched_aud,detected_series_row_labels,detected_series_row_labels_jp,detected_split_series_row_labels,detected_split_series_row_labels_jp,mature_issues,dueDatesByIssue):
    with open('jsda.AUD','w',newline='',encoding='shift-jis') as file:
        file.write(datetime.strftime(datetime.now(),"%m/%d/%Y %X"))
        file.write('\n')
        file.write('Dates Processed:\n')
        file.write('\n'.join(dates))
        file.write('\n===========================================================================================\n')
        file.write('Series not coded for:\n')
        file.write('\n'.join(not_matched_aud))
        file.write('\n===========================================================================================\n')
        file.write('Series detected:\n')
        for key in detected_series_row_labels.keys():
            file.write(f'{key}  ---  {detected_series_row_labels[key]}\n')
        for key in detected_series_row_labels_jp.keys():
            file.write(f'{key}  ---  {detected_series_row_labels_jp[key]}\n')
        for key in detected_split_series_row_labels.keys():
            file.write(f'{key}  ---  {detected_split_series_row_labels[key]}\n')            
        for key in detected_split_series_row_labels_jp.keys():
            file.write(f'{key}  ---  {detected_split_series_row_labels_jp[key]}\n')            
        file.write('\n===========================================================================================\n')
        file.write('Mature series:\n')
        for issue in mature_issues:
            file.write(f'{issue} --- Due date: {dueDatesByIssue[issue]}\n')

def main():
    print('~JSDA 2024 ~')
    today=datetime.today()
    prevmonth=(datetime.today()-relativedelta(months=1))
    today_str=datetime.strftime(today,'%Y%m%d')
    prevmonth_str=datetime.strftime(prevmonth,'%Y%m%d')
    
    parser=argparse.ArgumentParser(
    prog='JSDA',
    description='Downloads and procecsses Japanese Government Bond data from JSDA. Creates dictionaries from downloaded csvs and detects new and matured series.')
    parser.add_argument('-sd','--start_date',type=str,default=prevmonth_str)
    parser.add_argument('-ed','--end_date',type=str,default=today_str)
    parser.add_argument('-n','--no_output_mode',action='store_true')
    args=parser.parse_args()
    if args.no_output_mode: 
        print("No output mode activated, will not initiate add and disc proceedures.")
        no_output_mode=True
    else:
        no_output_mode=False
    start_date=args.start_date
    end_date=args.end_date
    start_year=int(start_date[0:4])
    end_year=int(end_date[0:4])

    dates=[]
    def run_gen_dates(year):
        gen_dates(year,start_date,end_date,dates)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(run_gen_dates, year) for year in range(start_year,end_year+1)]
        concurrent.futures.wait(futures)
    # print("Dates in specified range, scraped from JSDA: ")
    # print(dates)
    if '2002.8.2' in dates: dates.remove('2002.8.2')    
    if '2002.8.5' in dates: dates.remove('2002.8.5')
    if '2017.3.13' in dates: dates.remove('2017.3.13') 
    
    get_TBILL_tenors(start_year,end_year)
    
    if end_year!=start_year:
        endYearNotFound=True
        for date in dates:
            if end_year == int(date[0:4]):
                endYearNotFound=False
                break    
        if endYearNotFound:
            end_year-=1 #catches in beginning of year when new year data hasnt started yet
        
        startYearNotFound=True
        for date in dates:
            if start_year == int(date[0:4]):
                startYearNotFound=False
                break   
        if startYearNotFound: 
            start_year+=1 #catches in end of year
    
    LENGTH = len(dates)  # Number of iterations required to fill pbar
    pbar = tqdm(total=LENGTH, desc='downloads progress')  # Init pbar
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(get_csvs, date) for date in dates]
        for _ in concurrent.futures.as_completed(futures):
            pbar.update(n=1)
        concurrent.futures.wait(futures)
    with concurrent.futures.ProcessPoolExecutor(max_workers=25) as executor:
        try:
            futures = [executor.submit(get_csvs, date) for date in dates]
            for _ in concurrent.futures.as_completed(futures):
                pbar.update(n=1)
            for future in concurrent.futures.as_completed(executor.map(get_csvs, dates, timeout=300), timeout=180):
                print(future.result(timeout=180))
        except concurrent.futures._base.TimeoutError:
            print("This took to long...")
            stop_process_pool(executor)
        except AttributeError:
            pass 
        
    print('Downloads complete')
    datesByYear=sortDatesByYear(dates)
    print(list(datesByYear.keys()))
    def run_assemble_csvs(year):
        assemble_csvs(year,datesByYear)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(run_assemble_csvs, year) for year in datesByYear.keys()]
        concurrent.futures.wait(futures)
    assemble_master_csv(start_year,end_year)
    
    mature_issues=identifyMatureIssues(start_year,end_year,today_str)
    dueDatesByIssue=getAllDueDates(start_year,end_year)
    dueDatesByCode={}
    securityIdsByIssue=getAllSecurityIds(start_year,end_year)
    securityIdsByCode={}
    
    detected_series_row_labels={}
    detected_series_row_labels_jp={}
    detected_split_series_row_labels={}
    detected_split_series_row_labels_jp={}
    disc_series=[]
    disc_split_series=[]
    codes_by_year={}
    not_matched_aud=[]
    issueLabelsByCode={}
    
    for year in range(start_year,end_year+1):
        detected,disc,not_matched,detected_splits,disc_splits=compileCodes(year,mature_issues,dueDatesByIssue,dueDatesByCode,securityIdsByIssue,securityIdsByCode,issueLabelsByCode)
        if int(year) <= 2015:
            detected_series_row_labels_jp.update(detected)
            detected_split_series_row_labels_jp.update(detected_splits)
        else:    
            detected_series_row_labels.update(detected)
            detected_split_series_row_labels.update(detected_splits)
        for code in disc:
            disc_series.append(code)
        for code in disc_splits:
            disc_split_series.append(code)
        for line in not_matched:
            not_matched_aud.append(line)
        codes_by_year[year]=[detected.keys()]
    disc_series=set(disc_series)
    disc_split_series=set(disc_split_series)
     
    series_on_network=[]
    #series_on_network=getCodes("J74","INTDAILY")    #### CHANGE THIS WHEN ADD IS DONE
    detected_series_existing_by_year={}
    new_series=[]
    new_series_by_year={}
    for year in range(start_year,end_year+1):
        for code in codes_by_year[year][0]:
            if code in series_on_network:
                if year not in detected_series_existing_by_year.keys():
                    detected_series_existing_by_year[year]=[]
                detected_series_existing_by_year[year].append(code)
            else:
                new_series.append(code)
                if year not in new_series_by_year.keys():
                    new_series_by_year[year]=[]
                new_series_by_year[year].append(code)
                    
    new_series=set(new_series)
    # print('new series',new_series)
    
    new_split_series,existing_split_series=sortSplits(detected_split_series_row_labels,series_on_network)    
    _new_split_series,_existing_split_series=sortSplits(detected_split_series_row_labels_jp,series_on_network)
    new_split_series=new_split_series.union(_new_split_series)
    existing_split_series=existing_split_series.union(_existing_split_series)
    
    disc_existing_split_series=set()
    for series in existing_split_series:
        if series in disc_split_series:
            disc_existing_split_series.add(series)
    new_splits_by_due_year=getSplitYears(new_split_series,dueDatesByCode)
    existing_splits_by_due_year=getSplitYears(existing_split_series,dueDatesByCode)
    
    setSplitCodeMaxYearToEndYear(new_splits_by_due_year,end_year)
    setSplitCodeMaxYearToEndYear(existing_splits_by_due_year,end_year)
    
    new_splits_all=genSplitCodes(new_splits_by_due_year)
    existing_splits_all=genSplitCodes(existing_splits_by_due_year)
    
    disc_existing_series=set()
    for series in disc_series:
        if series in series_on_network:
            disc_existing_series.add(series)
    
    _new=new_series.union(new_splits_all)
    _disc=disc_existing_series.union(disc_existing_split_series)  

    if len(_disc) > 0:
        series_on_network_matured=checkDisc(_disc,series_on_network)
        with open('jsda/disc.lst','w',newline='') as file:
            for series in series_on_network_matured:
                if series in series_on_network:
                    file.write(str(series)+'\n')
    else:
        series_on_network_matured=None
    
    if no_output_mode:
        genJsdaYearDict(detected_series_existing_by_year,detected_series_row_labels,detected_series_row_labels_jp)
        csv2modb(range(start_year,end_year+1))

        genNifDict(new_series_by_year,detected_series_row_labels,detected_series_row_labels_jp)
        for year in new_series_by_year.keys():
            csv2modb_nif(year)
        genJsdaYearSplitDict(existing_splits_by_due_year,detected_split_series_row_labels,detected_split_series_row_labels_jp)
        for year in existing_splits_by_due_year.keys():
            csv2modb_Split(year)
        genJsdaYearSplitDict_n(new_splits_by_due_year,detected_split_series_row_labels,detected_split_series_row_labels_jp)
        for year in new_splits_by_due_year.keys():
            try:
                csv2modb_Split_n(year)
            except FileNotFoundError:
                continue
        outputJsdaAud(dates,not_matched_aud,detected_series_row_labels,detected_series_row_labels_jp,detected_split_series_row_labels,detected_split_series_row_labels_jp,mature_issues,dueDatesByIssue)
        
    
    if no_output_mode==False:
        new_labels=genLabels2(_new,dueDatesByCode,securityIdsByCode,issueLabelsByCode,today_str)
        disc_labels=genLabels2(_disc,dueDatesByCode,securityIdsByCode,issueLabelsByCode,today_str)
        writeLabels(new_labels,disc_labels)    
        
        genJsdaYearDict(detected_series_existing_by_year,detected_series_row_labels,detected_series_row_labels_jp)
        csv2modb(list(detected_series_existing_by_year.keys()))
        
        genJsdaYearSplitDict(existing_splits_by_due_year,detected_split_series_row_labels,detected_split_series_row_labels_jp)
        for year in existing_splits_by_due_year.keys():
            csv2modb_Split(year)
        
        outputJsdaAud(dates,not_matched_aud,detected_series_row_labels,detected_series_row_labels_jp,detected_split_series_row_labels,detected_split_series_row_labels_jp,mature_issues,dueDatesByIssue)
                
        if os.path.exists('jsda/disc.lab'):
            runDisc()
        
        if len(_new)>0:   
            genNewPar(_new)
            genAddYearDict(new_series_by_year,detected_series_row_labels,detected_series_row_labels_jp)
            genAddYearSplitDict(new_splits_by_due_year,detected_split_series_row_labels,detected_split_series_row_labels_jp)
            runAdd()
    
if __name__=="__main__":
    main()


#  Go to website, scrape dates 
#  https://market.jsda.or.jp/en/statistics/bonds/prices/otc/index.html 
#  https://market.jsda.or.jp/en/statistics/bonds/prices/otc/archive2022.html
  
#  download csvs using dates:
#  https://market.jsda.or.jp/{root}/{YYYY}/{m}/{E}{y}{m}{day}.csv
#  saves to downloads folder
  
#  group csvs by year, add header, only looking at types 1 3 and 5
#  saves to compiled folder
  
#  downloads TBILL tenors from another document
#  , generates a dictionary that matches tbill numbers to tenors
#  https://www.mof.go.jp/english/policy/jgbs/auction/past_auction_results/Auction_Results_for_JGBs.xls
#  https://www.mof.go.jp/english/policy/jgbs/auction/past_auction_results/Auction_Results_for_T-bills.xls
#  https://www.mof.go.jp/english/policy/jgbs/auction/calendar/2312e.htm
#  saves to files folder
  
#  goes through each yearly csv and identifies the due date and security id for each
  
#  generates a code for each security. for csvs before a certain date,
#  only japanese files are available, so files are translated. 
#  the generation of codes from issues is handled by the Security class.
#  split codes will be generated for securities with a tenor over 7 years. (needs to be added)
  
#  using due dates, mature securities are identified. these series are added to a list of 
#  disc series
  
#  using dlxlist, new codes that are not on db are identified.
#  they are added to a list called new series
  
#  a dict entry is created for all series.
  
#  non-new series are put into dicts to be processed in main update
#  class GenFiles:
#  add.dic is created for new series -> add/add.dic
#  labels are created for disc and new series -> add/new.lab  jsda/disc.lab
#  params are created for new series -> add/new.par
  
#  csv2modb for main update, process as normal 
#  a seperate DOS opens to process add.
#  a seperate DOS opens to discontinue mature securities.

