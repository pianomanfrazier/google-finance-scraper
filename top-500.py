import sys
import urllib2
import csv
import threading
from bs4 import BeautifulSoup

WIKI = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
lock = threading.Lock()

def get_tickers(site):
    """return dictionary {industry:[tickers]}"""
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib2.Request(site, headers=hdr)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page, "html.parser")

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers

def get_csv(industry, ticker):
    """
    get a csv from google finance for the ticker
    returns a csv reader object
    """
    base = 'https://www.google.com/finance/historical?q='
    options = '&startdate=Jan+1%2C+2001&enddate=Dec+31%2C+2016&num=30&ei=Wcx3WKj4Gcn_jAHphIqQBg'
    filetype = '&output=csv'

    url = base + ticker + options + filetype
    attempt_limit = 20
    for attempt in range(1, attempt_limit+1):
        try:
            print "Fetch attempt {}, {} in {} ...".format(attempt, ticker, industry)
            stock_data = urllib2.urlopen(url)
            cvs_reader = csv.reader(stock_data)
            append_csv(cvs_reader, industry, ticker)
        except Exception as error:
            print "Invalid url: {}".format(url)
            print "Error: {}".format(error)
        else:
            print "Fetch {} in {} successful in {} attempts!".format(ticker, industry, attempt)
            break
    else:
        print "Failed all {} attempts to get {} in {}".format(attempt_limit, ticker, industry)

def append_csv(cvs_reader, industry, ticker):
    """append csv to output.csv with industry and ticker columns added"""
    if cvs_reader is None:
        return
    lock.acquire()
    try:
        with open(sys.argv[1], "a") as fout:
            writer = csv.writer(fout, lineterminator="\n")
            all_rows = []
            cvs_reader.next() #skip the first row
            for row in cvs_reader:
                row.append(ticker)
                row.append(industry)
                all_rows.append(row)
            writer.writerows(all_rows)
            print "Appended {} : {} to {}".format(industry, ticker, sys.argv[1])
    finally:
        lock.release()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Provide a filename to output csv")

    threads = []
    for industry, tickers in get_tickers(WIKI).iteritems():
        print  "Fetching industry: {}".format(industry)
        for ticker in tickers:
            threads.append(threading.Thread(target=get_csv, args=(industry, ticker,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
