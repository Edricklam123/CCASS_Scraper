# import libraries
from bs4 import BeautifulSoup # as bs
import requests
import pandas as pd
import datetime


# DATA CLEANING
def read_html(html_text):
    return pd.read_html(html_text)[1]


def clean_df(df):
    df["Stock Code"] = df["Stock Code"].str.strip('Stock Code: ')+" HK"
    df['Shareholding in CCASS'] = df['Shareholding in CCASS'].str.strip('Shareholding in CCASS:').replace(',', '',regex=True)
    df['Stock Name'] = df['Stock Name'].str.strip('Stock Name: ')
    df['% of the total number of Issued Shares'] = df['% of the total number of Issued Shares'].str.strip('% of the total number of Issued Shares:')+'%'
    df = df.drop('Stock Name', axis=1).set_index('Stock Code')
    return df


# Holding Shares Summary
def create_df_HSS(df, tb_date):
    df_HSS = df.drop("% of the total number of Issued Shares", axis=1)
    df_HSS = df_HSS.rename(columns={'Shareholding in CCASS': tb_date})
    return df_HSS

def merging_df_HSS(df, tb_date):
    global df_HSS, DATE
    temp = df.drop("% of the total number of Issued Shares", axis=1)
    temp = temp.rename(columns={'Shareholding in CCASS': tb_date})
    # check if tb_date already exist
    if tb_date in df_HSS.columns:
        print(f"[ERR] {tb_date} already exist, maybe {DATE} is a public holidy")
        return
    df_HSS = df_HSS.merge(temp, how="outer", on="Stock Code")
    # print(f"Added into the table")

# PCT PART
def create_df_PCT(df, tb_date):
    df_HSS = df.drop("Shareholding in CCASS", axis=1)
    df_HSS = df_HSS.rename(columns={'% of the total number of Issued Shares': tb_date})
    return df_HSS

def merging_df_PCT(df, tb_date):
    global df_PCT, DATE
    temp = df.drop("Shareholding in CCASS", axis=1)
    temp = temp.rename(columns={'% of the total number of Issued Shares': tb_date})
    # check if tb_date already exist
    if tb_date in df_PCT.columns:
        print(f"[ERR] {tb_date} already exist, maybe {DATE} is a public holidy")
        return
    df_PCT = df_PCT.merge(temp, how="outer", on="Stock Code")
    # print(f"Added into the table")

# LOOP for different DATES
def get_html(date):
    data = {
        "__VIEWSTATE": '/wEPDwUJNjIxMTYzMDAwZGSFj8kdzCLeVLiJkFRvN5rjsPotqw==',
        "__VIEWSTATEGENERATOR": '3C67932C',
        "__EVENTVALIDATION": '/wEdAAdbi0fj+ZSDYaSP61MAVoEdVobCVrNyCM2j+bEk3ygqmn1KZjrCXCJtWs9HrcHg6Q64ro36uTSn/Z2SUlkm9HsG7WOv0RDD9teZWjlyl84iRMtpPncyBi1FXkZsaSW6dwqO1N1XNFmfsMXJasjxX85ju3P1WAPUeweM/r0/uwwyYLgN1B8=',
        "today": '20210415',
        "sortBy": 'stockcode',
        "sortDirection": 'asc',
        "alertMsg": '',
        "txtShareholdingDate": date,
        "btnSearch": 'Search',
    }
    response = requests.post(url, data=data).text
    return response

def get_data(html, type):
    """
    input: html script;  type = {"HSS", "PCT"}
    output: df cleaned
    :param date:
    :return:
    """
    soup = BeautifulSoup(html, 'lxml')
    tb_date = soup.find(style="text-decoration:underline;").text.strip('Shareholding Date: ')
    df = read_html(html)
    df = clean_df(df)      #take out strings and leave columns: "Stock Code, Shareholding in CCASS	% of the total number of Issued Shares"
    if type == "HSS":
        merging_df_HSS(df, tb_date)
    elif type == "PCT":
        merging_df_PCT(df, tb_date)
    return

if __name__ == "__main__":

    DATE = "2020/08/01"
    TODAY = datetime.datetime.today()

    # WEB SCRAPPING
    url = "https://www.hkexnews.hk/sdw/search/mutualmarket.aspx?t=hk"  # English website
    headers = {
        "__VIEWSTATE": '/wEPDwUJNjIxMTYzMDAwZGSFj8kdzCLeVLiJkFRvN5rjsPotqw==',
        "__VIEWSTATEGENERATOR": '3C67932C',
        "__EVENTVALIDATION": '/wEdAAdbi0fj+ZSDYaSP61MAVoEdVobCVrNyCM2j+bEk3ygqmn1KZjrCXCJtWs9HrcHg6Q64ro36uTSn/Z2SUlkm9HsG7WOv0RDD9teZWjlyl84iRMtpPncyBi1FXkZsaSW6dwqO1N1XNFmfsMXJasjxX85ju3P1WAPUeweM/r0/uwwyYLgN1B8=',
        "today": TODAY.strftime("%Y%m%d"),
        "sortBy": 'stockcode',
        "sortDirection": 'asc',
        "alertMsg": '',
        "txtShareholdingDate": DATE,
        "btnSearch": 'Search',
    }
    response = requests.post(url, data=headers).text
    soup = BeautifulSoup(response, 'lxml')
    tb_date = soup.find(style="text-decoration:underline;").text.strip('Shareholding Date: ')
    print(f"[SYS] Initialise from {tb_date}")

    df = read_html(response)
    df = clean_df(df)
    df_HSS = create_df_HSS(df, tb_date)
    df_PCT = create_df_PCT(df, tb_date)

    daterange = pd.bdate_range(DATE, TODAY)
    for i in daterange:
        DATE = i.strftime("%Y/%m/%d")
        # print(date)
        soup = get_html(DATE)
        get_data(soup, "HSS")
        get_data(soup, "PCT")
        print(f"[SYS] Done {DATE}")

    print("[SYS] --------------- Done merging ---------------")

    # FINAL Pollishing
    df_HSS.index = df_HSS.index.str.strip(' HK').astype(int)
    df_HSS = df_HSS.sort_index()
    df_HSS.index = df_HSS.index.astype(str) + ' HK'
    df_HSS.index.name = 'Date'

    df_PCT.index = df_PCT.index.str.strip(' HK').astype(int)
    df_PCT = df_PCT.sort_index()
    df_PCT.index = df_PCT.index.astype(str) + ' HK'
    df_PCT.index.name = 'Date'

    print(df_HSS.transpose())
    print(df_PCT.transpose())

    # EXPORT
    writer = pd.ExcelWriter(r'D:\Downloads\CCASS.xlsx')  # can use append later on to add record
    df_HSS.transpose().to_excel(writer, sheet_name="Holding Shares Summary")
    df_PCT.transpose().to_excel(writer, sheet_name="Pct Summary")
    writer.save()
    writer.close()