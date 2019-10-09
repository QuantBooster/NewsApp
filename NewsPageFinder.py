# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 15:54:57 2019

@author: a.mohammadi
"""


import pymysql
from pandas import read_sql
connString = "db01.cdstk7wwp9vq.us-east-1.rds.amazonaws.com:Quant:Booster8351:QB"

from re import search as ReSearch
from urllib.parse import urlparse
from lxml.html import fromstring
from tqdm import tqdm
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from requests import get
from collections import OrderedDict
import networkx as nx
from datetime import datetime
from warnings import filterwarnings
filterwarnings("ignore")

from logging import getLogger, DEBUG, Formatter, FileHandler
log = getLogger('NewsApp')
log.propagate = False
log.setLevel( DEBUG )
fh = FileHandler(f"_log_NewsPageFinder", encoding='utf-8-sig')
formatter = Formatter('%(levelname)s _,_ %(asctime)s _,_ %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)

from spacy import load
from spacy_langdetect import LanguageDetector
nlp = load('en_core_web_sm')
nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)

#%%
def DetectLanguage(text):
    doc = nlp(text)
    # print(doc._.language)
    return doc._.language['language']
#%%    
def GetHTMLTree( url, br ):
    br.get( url )
    url = br.current_url
    html = fromstring( br.page_source , base_url=url)   
    html.make_links_absolute(url, resolve_base_href=True)
    
    ### Rewrite html considering frames
    nhtml = fromstring( "<!DOCTYPE html><html><body></body>\n</html>\n" )
    for el in html.xpath("//body/*"):
            nhtml.body.append(el)
            
    for f in br.find_elements_by_xpath("//frame|//iframe"):
        try:
            br.switch_to.frame(f)
            html = fromstring( br.page_source , base_url=br.current_url)   
            html.make_links_absolute(br.current_url, resolve_base_href=True)
            for el in html.xpath("//body/*"):
                nhtml.body.append(el)
            br.switch_to.default_content()
        except:
            log.exception( url )
    
    return nhtml, url

#%%
# Remove bad elements
XpathToBeRemoved =  "//*[contains(@*,'cookie')]" + "|//*[contains(@*,'banner')]"\
                +"|//comment()|//link|//style|//noscript|//script|//javascript"\
                +"|//head|//meta|//frame|//iframe|//input|//button"
                
def GetEnglishHTML( co, br ):
    
    html, url = GetHTMLTree( co['coWebsite'], br )
    co['domain'] = urlparse( url ).netloc.split('www.')[-1]
    for bad in html.xpath( XpathToBeRemoved ):
        bad.getparent().remove(bad)      
    lang = DetectLanguage( GetText( html ) )
    
    if lang!='en':
        url = ''
        # search for first a which has english text
        processed = set()
        xpath = "//body//a[starts-with(@href,'http')]"
        for i, a in enumerate( html.xpath( xpath ) ):   
            href = a.get('href')
            if href in processed or i>50 or co['domain'] not in href:
                continue
                
            atext = GetText(a)
            # print(href, atext)
            if ( len(atext)==0 or atext.lower().startswith('en') ):
                try:
                    r = get(href, headers={"Range":"bytes=0-100"}, verify=False)
                    if b'<html ' in r.content:
                        html, url = GetHTMLTree( href, br )
                        for bad in html.xpath( XpathToBeRemoved ):
                            bad.getparent().remove(bad)      
                        lang = DetectLanguage( GetText( html ) )
                        if lang.startswith('en'):
                            break
                        else:
                            url = ''
                except:
                    log.exception( href )
            
            processed.add( href )
            
            co['timeStop'] = datetime.now().timestamp()
            if co['timeStop'] - co['timeStart'] > co['timeOut']:
                break

    co['urlEn'] = url
    
    for wh in br.window_handles[1:]:
        br.switch_to.window(wh)
        br.close()
    br.switch_to.window(br.window_handles[0])
    
    return co
#%%
def GetText( el ):
    tlst = []
    for t in el.itertext():
        t = t.replace('\xa0', ' ').strip(' \n\r\t')
        if t:
            tlst.append( t )
    return '\n'.join( tlst )

#%%
def FindNewsPage( co, gr ):
    
    newsPages = OrderedDict()
    for nw in ['news', 'release', 'press', 'announcement', 'circular', 'investor']:
        for url in gr: #break
            if '.pdf' not in url.lower():
                text = gr.node[url]['text']
                if nw in ' '.join( text ).lower():
                    newsPages[url] = text
                    # print(url, text)            
                    break
    
    co['newsPages'] = newsPages
    
    return co
#%%
def IsValidUrl( url ):
    if type(url) != type('aliakbar'):
        return False
    
    url = url.lower()
        
    if not url.startswith('http'):
        return False
    
    if len(url) > 800:
        return False
    
    if len( url.split('#') )>1:
        return False
    
    for w in ['google','linkedin','facebook','sitemap', 'youtube', 'flickr', 'reuters',
              'twitter', '/contact', '/about', '/security', '/career', 'rss',
              'webmail', 'sociallink', '.jpg', '.png', '.ico', '.css', '.xml', 
              '.mp4', '.mp3', 'wordpress', '.svg', '.zip', '.xls', '.rtf', '.pdf',
              'youtu.be', 'sharelink']:
        if w in url:
            return False
        
    return True
#%%
def IsValidTitle( text ):
    text = ' ' + text + ' '
    expression = "\W(news|announcements?|releases?|press|media|investors?|publications?)\W"
    if ReSearch(expression, text.lower()) is None:
        return False
    
    return True

#%%
def GetLinks( n, br ): 
    links = set()
    # TODO: do something about wait after get
    html, rurl = GetHTMLTree( n, br )
    # Remove bad elements
    for bad in html.xpath( XpathToBeRemoved ):
        bad.getparent().remove(bad)  
    
    # find target <a>
    for a in html.xpath( ".//body//a[@href]" ): 
        try:
            href = a.get('href')
#            if href == 'https://www.1300smiles.com.au/investors/asx-releases/': break
        
            if IsValidUrl( href ):
                text = GetText( a )
                if IsValidTitle( text ):
                    links.add( (text, href) )
#            print(href)
        except:
            log.exception( n )
    
    return links
#%%
def ExtendGraph( gr, urls, br, level ):
    # TODO: use multithreading or async processing
    for inode, n in enumerate(urls): #break
        ########### Collect links
        try:
            links = GetLinks( n, br )
        except:
            log.exception( f"{gr.graph['coWebsite']} \t {n}" )
            
        gr.node[n]['text'].add( br.title )
            
        ##########    
        # for text, url in tqdm(links, desc=f'{level}:{inode}'): #break
        for text, url in links: #break
            if gr.graph['domain'] not in urlparse( url ).netloc:
                continue
            
            if gr.has_edge(n, url) or gr.has_edge(url, n):
                # gr[n][url]['w'] += 1
                if len( text ):
                    gr.node[url]['text'].add(text) 
                continue
            # print(url)
            try:
                r = get(url, headers={"Range":"bytes=0-200"}, timeout=10, verify=False)
                if b'<html' not in r.content:
                    continue
            except:
                log.exception( url )
            
            gr.add_edge(n, url, w=1)
            gr.node[url]['text'] = {text,}
            gr.node[url]['level'] = level
                
    return gr
                    
#%%
def GetCompanyWebsiteGraph( co, br ):
    maxLevel = 5
    gr = nx.DiGraph( **co )
    
    if gr.graph['urlEn'].lower().startswith('http'):
        gr.add_node( gr.graph['urlEn'], level=0, text=set() )
    else:
        gr.add_node( gr.graph['coWebsite'], level=0, text=set() )
        
    for level in range( 1, maxLevel ): #level = 1        
        urls = [n for n in gr if gr.node[n]['level']==level-1] #and gr.out_degree(n)==0
        # print(level, len(urls))
        gr = ExtendGraph( gr, urls, br, level )
        
        co['timeStop'] = datetime.now().timestamp()
        if co['timeStop'] - co['timeStart'] > co['timeOut']:
            break

    return gr
#%%
def DoCo( co ):  # co = companies[18]; co = DoCo( co )
    co['timeOut'] = 10*60
    co['timeStart'] = datetime.now().timestamp()
    co['newsPages'] = {}

    try:
        options = Options()
        options.headless = True
        options.set_preference("security.sandbox.content.level", 5)
        br = Firefox( options=options )
        br.set_page_load_timeout( 60 )
    except:
        log.exception( co['coWebsite'] )
        co['timeStop'] = co['timeStart']
        return co

    try:
        co = GetEnglishHTML( co, br )
        gr = GetCompanyWebsiteGraph( co, br )
        co = FindNewsPage( co, gr )
    except:
        log.exception( co['coWebsite'] )
    
    br.quit()
    co['timeStop'] = datetime.now().timestamp()
    
    return co

#%%
def MultiProcess( companies ):
    
    from multiprocessing import Pool
#    from pandas import DataFrame as DF
    
    results = []
    with Pool( processes=10 ) as pool:
        for co in tqdm( pool.imap_unordered(DoCo, companies), 
                        total=len(companies), desc="Doing Companies" ):
#            newsPages = co['newsPages']
#            for url, texts in newsPages.items(): 
#                r = co.copy()
#                r.pop('newsPages', None)
#                r['newsUrl'] = url
#                r['newsPageTitle'] = ' \n'.join( texts )
#                results.append( r )
#                
#            if len(newsPages)==0:
#                r = co.copy()
#                r.pop('newsPages', None)
#                results.append( r )
#
#            DF(results).to_csv("NewsPageFinderRes.csv", index=False, encoding='utf-8-sig')
            
            InsertIntoDB( co )
                
    print( "All done!!!" )
    return results

#%%
def InsertIntoDB( co ):
    try:
        conn = pymysql.connect(*connString.split(':'), 3306)
    except:
        log.exception( co['coWebsite'] )
        return
    
    for url, texts in co['newsPages'].items():
        try:
            if url not in newsUrls:
                newsUrls.add(url)
                url = url.replace('\'', '\'\'')
                q = fr"""insert into newsApp (coId, newsUrl, isValid) 
                        values ({co['coId']}, '{url}', 1)"""
                with conn.cursor() as c:
                    c.execute(q)
                    conn.commit()
        except:
            log.exception( co['coWebsite'] )
        
    try:
        conn.close()
    except:
        log.exception( co['coWebsite'] )

    return
        

#%%
def GetCompanyList():
    conn = pymysql.connect(*connString.split(':'), 3306)
    q = """select * from companies c left join newsApp na on c.coID = na.coID"""
    df = read_sql(q, conn)
    conn.close()

    companies = df.loc[:,~df.columns.duplicated()]
    newsUrls = set( companies['newsUrl'] )
    
    return companies.to_dict('records'), newsUrls

#%%
def Analysis():
    from pandas import read_csv, to_datetime
    df = read_csv("NewsPageFinderRes - Copy.csv", encoding = "utf-8-sig")
    df['t0'] = to_datetime( df['timeStart'] )
    df['t1'] = to_datetime( df['timeStop'] )
    df['time'] = df['t1'] - df['t0']
    df = df.sort_values(['time'])
#%%
if __name__=="__main__":
    
    companies, newsUrls = GetCompanyList()
    
    res = MultiProcess( companies )
    