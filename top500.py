import sys
import urllib2
import csv
import threading
import logging
from statusbar import StatusBar
from bs4 import BeautifulSoup

logging.basicConfig(filename="debug.log",
                    format='%(levelname)s: %(relativeCreated)6d %(threadName)s - %(message)s',
                    level=logging.DEBUG)

WIKI = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
lock = threading.Lock()

# taken from http://www.thealgoengineer.com/2014/download_sp500_data/
def get_tickers(site):
    """return dictionary {industry:[tickers]}"""
    #print "Fetching top 500 tickers from Wikipedia ..."
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
    global sbar
    base = 'https://www.google.com/finance/historical?q='
    options = '&startdate=Jan+1%2C+2001&enddate=Dec+31%2C+2016&num=30&ei=Wcx3WKj4Gcn_jAHphIqQBg'
    filetype = '&output=csv'

    url = base + ticker + options + filetype
    attempt_limit = 30
    sbar.updatemsg("Fetching {} in {} ...".format(industry, ticker))
    for attempt in range(1, attempt_limit+1):
        try:
            logging.debug("fetch attempt %s, %s in %s ...", attempt, ticker, industry)
            stock_data = urllib2.urlopen(url)
            cvs_reader = csv.reader(stock_data)
            append_csv(cvs_reader, industry, ticker)
        except Exception as error:
            logging.debug("Invalid url: %s", url)
            logging.debug("Error: %s", error)
            sbar.updatemsg("{}".format(error))
        else:
            #print "Fetch {} in {} successful!".format(ticker, industry)
            logging.info("%s in %s fetched in %s attempts", ticker, industry, attempt)
            break
    else:
        logging.debug("Failed all %s attempts to get %s in %s", attempt_limit, ticker, industry)
        #print "Fetch {} in {} failed".format(ticker, industry)

def append_csv(cvs_reader, industry, ticker):
    """append csv to output.csv with industry and ticker columns added"""
    global sbar
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
            logging.info("Appended %s : %s to %s", industry, ticker, sys.argv[1])
            sbar.updateone(msg="{} : {}".format(industry, ticker))
    finally:
        lock.release()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Provide a filename to output csv")

    threads = []
    for industry, tickers in get_tickers(WIKI).iteritems():
        for ticker in tickers:
            threads.append(threading.Thread(target=get_csv, args=(industry, ticker,)))

    with StatusBar(len(threads)) as sbar:
      for thread in threads:
          thread.start()
      for thread in threads:
          thread.join()
