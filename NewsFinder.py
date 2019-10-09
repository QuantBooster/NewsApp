# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 12:11:51 2019

@author: a.mohammadi
"""
import logging, io, traceback
class MyFormatter(logging.Formatter):
    def __init__(self, fmt):
        logging.Formatter.__init__(self, fmt)
    def formatException(self, ei):
        sio = io.StringIO()
        traceback.print_exception(ei[0], ei[1], ei[2], None, sio, chain=False)
        s = sio.getvalue()
        sio.close()
        return s
logFileName = f"_log_NewsFinder"
log = logging.getLogger( logFileName )
log.propagate = False
log.setLevel( logging.DEBUG )
fh = logging.FileHandler(logFileName, encoding = "UTF-8-sig")
formatter = MyFormatter('%(levelname)s _,_ %(asctime)s _,_ %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)

from datetime import datetime
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from lxml.html import fromstring
from requests import get
import subprocess as sp
from re import sub as resub

from warnings import filterwarnings
filterwarnings("ignore")

cdate = datetime.now()

from date_extractor import extract_dates       
import networkx as nx
from os.path import isfile
from os import remove
from uuid import uuid4
from lxml.html import tostring
from time import sleep
from gensim.summarization.summarizer import summarize

from pandas import DataFrame as DF
import pymysql

#%%
class News(dict):
    def __init__(self):
        self['exists'] = False
        for k in ['fpath', 'qpath', 'title', 'pdate','url', 'summary',
                  'urlHtml', 'urlPdf', 'textHtml', 'textPdf',
                  'iosps', 'cntrIds', 'coIds', 'siteIds', 'siteId', 'commIds',
                  'html']:
            self[k] = ''
        
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)

#%%
connString = "db01.cdstk7wwp9vq.us-east-1.rds.amazonaws.com:Quant:Booster8351:QB"
def SQLExec( q ):
    try:
        conn = pymysql.connect( *connString.split(':'), 3306,
                               cursorclass=pymysql.cursors.DictCursor )
        with conn.cursor() as c:
            c.execute( q )
            rows = list( c.fetchall() )
        conn.close()   
        return rows
    except:
        log.exception( f"SQLExec Error: {q}" )
        try:
            conn.close()
        except:
            pass
        return []
#%%
def NewsUrlsTexts( p ):  
    newsUrls, newsTitles = set(), set()
    q = f"""select nUrl, nTitle from news where coId={p['coId']} """            
    for n in SQLExec( q ):
        newsUrls.add( n['nUrl'] )
        newsTitles.add( n['nTitle'] )
                
    return newsUrls, newsTitles
#newsUrls, newsTexts = NewsUrlsTexts( p )

#%%
def GetText( el ):
    tlst = []
    for t in el.itertext():
        t = t.replace('\xa0', ' ').strip(' \n\r\t')
        if t:
            tlst.append( t )
    return '\n'.join( tlst )
#%%    
def GetHtmlTree( url, br ): # url=p['URL']
    #url = n.url # from lxml.html import tostring
    br.get( url )
    sleep(2)
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
#            log.exception( url )
            pass
    
    for wh in br.window_handles[1:]:
        br.switch_to.window(wh)
        br.close()
        br.switch_to.window( br.window_handles[0] )
        
    html = nhtml 
    # tostring( html )
    return html, url  
#%%
def ParseDateStr(dstr):
    if len(dstr)==0:
        return ''
    dstr = ' ' + dstr.lower() + ' '
    dstr = resub('[\W\d](th|st|nd|rd)\W', '', dstr).strip()

    try:
        dobjs = extract_dates(dstr, return_precision=True, debug=False)
        for o in dobjs:
            if o[1]=='day': 
                dobj = o[0].replace(tzinfo=None)
                if dobj <= cdate:
                    return dobj
    except:
        return ''
    
    return ''
#%%        
# dstr = "04/09/2018"
def FindPublishDate( dstr ):
    for line in dstr[:300].split('\n\r\t'):        
        line = resub("\W+", ' ', line).strip()
        pdate = ParseDateStr( line )
        if pdate:
            return pdate
        
    return ''
#%%
def ExtractText(n):
    
    try:
        command_list = [r"pdftotext", "-f", "1", "-l", "5", n.fpath, "-", ] # "-enc", "UTF-8",
        n.textPdf = sp.run(command_list, timeout=60, 
                        stderr=sp.STDOUT, check=True, 
                        stdout=sp.PIPE).stdout.decode("Latin1").strip()
    except:
        n.qpath = n.fpath.split('.pdf')[0] + '_qp.pdf'
        command_list = [r"qpdf", "--decrypt", "--suppress-recovery", n.fpath, n.qpath]
        sp.run(command_list, timeout=60, stderr=sp.STDOUT, stdout=sp.PIPE, check=True)
        command_list = [r"pdftotext", "-f", "1", "-l", "5", n.qpath, "-", ] # "-enc", "UTF-8",
        n.textPdf = sp.run(command_list, timeout=60, 
                stderr=sp.STDOUT, check=True, 
                stdout=sp.PIPE).stdout.decode("Latin1").strip()

    return n
    
#%%
def DoPdf(n):       
    r = get(n.urlPdf, timeout=60, verify=False, stream=True)
    n.fpath = fr".\tmp\{str(uuid4())}.pdf"
    with open(n.fpath, 'ab') as fd:
        sleep(1)
        for i, chunk in enumerate( r.iter_content(chunk_size=1024) ): #break
            if i==0:
                n.chunk = chunk
                if not n.chunk.startswith( b'%PDF' ):
                    n.urlPdf = ''
                    break
            fd.write(chunk)
    
    if n.urlPdf != '':
        try: 
            n = ExtractText(n)            
        except:
            log.exception( n.urlPdf )
            
            # temporary file may have been created in DoPdf(n). So delete them.
    if isfile(n.fpath):
        remove(n.fpath)
    if isfile(n.qpath):
        remove(n.qpath)
    return n

#%%
def DoHtml(n, br):
    
    html, n.urlHtml = GetHtmlTree( n.urlHtml, br ) 
    
    # Remove bad elements
    XpathToBeRemoved =  "//*[contains(@*,'cookie')]"\
    +"|//*[contains(@*,'banner')]"\
    +"|//script|//javascript|//nav|//head|//menu"\
    +"|//comment()|//link|//style|//noscript|//meta"\
    +"|//frame|//iframe|//input|//button"
    
    for bad in html.xpath( XpathToBeRemoved ):
        bad.getparent().remove(bad)        
    
    # Create DiGraph
    pXpath = "//p|//pre"
    elems = html.xpath( pXpath )
    G = nx.DiGraph()
    for el in elems:
        if len( GetText(el) ) > 15:
            for i, an in enumerate( el.iterancestors() ):
                if i < 1:
                    G.add_edge(an, el)
                    el = an
                else:
                    break
            
    # Get connected components and sort them based on the length of paragraphs
    cc = sorted( list( nx.connected_components( G.to_undirected() ) ), 
                key=lambda x: len(' '.join( [GetText(xi) for xi in x if xi.tag=='p'] ) ) )
    if len(cc) < 1:
        return n
        
    Gsub = G.subgraph( cc[-1] )
    top = [n for n, d in Gsub.in_degree() if d==0 ][0] # tostring(top)
    n.textHtml = GetText( top )
    n.html = tostring(top, encoding='unicode', method='html')
    # with open('News.html', 'w', encoding="UTF-8") as f: f.write(n.html)
    
    ##### Get master top node to find related PDF files and maybe a valid title     
    pSourceline = top.xpath( pXpath )[0].sourceline
    hxpath = ".//h1|.//h2|.//h3|.//h4|.//h5|.//h6" 
    articleXpath = ".//descendant-or-self::article"
    i = 2
    while i:
        headers = top.xpath( f"{hxpath}|{articleXpath}")
        if len( headers ) and (headers[0].sourceline - pSourceline < 0):
            break
        topParent = top.getparent()
        if topParent is None:
            break
        top = topParent
        i -= 1
    
    if not IsValidTitle(n.title):
        for h in top.xpath( hxpath ):
            txt =  GetText( h )
            if IsValidTitle( txt ):
                n.title = txt
                break
                
    #### Look for pdf files in master top node
    for a in top.xpath( ".//a[@href]" ): #print( a.get( 'href' ) )
        href = a.get( 'href' )
        # title = GetText( a )
        if IsValidUrl( href ): # and ('pdf' in href.lower() or 'pdf' in title.lower())
            n.urlPdf = href
            n = DoPdf( n )
            if n.urlPdf != '':
                break
            
    return n


#%%
def IsValidUrl( url ):
    if type(url) != type('ali'):
        return False
    
    url = url.lower()
        
    if not url.startswith('http'):
        return False
    
    if len(url) > 500 or len( url.split('/') ) < 4:
        return False
    
    if len( url.split('#') )>1:
        return False
    
    for w in ['google','linkedin','facebook','sitemap', 'youtube', 'flickr', 'reuters',
              'twitter', '/contact', '/about', '/security', '/career', 'rss',
              'webmail', 'sociallink', '.jpg', '.png', '.ico', '.css', '.xml', 
              '.mp4', '.mp3', 'wordpress', '.svg', '.zip', '.xls', '.rtf',
              'youtu.be', 'sharelink', ]: #'asx.com.au',
        if w in url:
            return False
        
    return True

#%%
def IsValidTitle( txt, 
                 expresion=r"[^a-zA-Z\s]|pdf|read|more|download|back|click"\
                 +"|pages?|view|continue|reading|format|opens?|window|new"\
                 +"|media|release|size|read|full|story"\
                 +"|january|february|march|april|may|june|july|august|september"\
                 +"|october|november|december|jan|feb|mar|apr|jun|jul"\
                 +"|aug|sep|oct|nov|dec"):
    txt = resub(expresion, '' , txt.lower())
    txt = resub(" +", ' ', txt)
    if len(txt.split())<2:
        return False
    return True

#%%
# TODO: non related <a> in the same lca ==> https://www.cobaltblueholdings.com/news/ 
def FindNews(p, br):
    html, url = GetHtmlTree( p['npUrl'], br )
    XpathToBeRemoved = "//*[contains(@*,'cookie')]"\
    +"|//*[contains(@*,'banner')]"\
    +"|//script|//javascript|//nav|//head|//menu"\
    +"|//comment()|//link|//style|//noscript|//meta"\
    +"|//frame|//iframe|//input|//button|//img|//svg"
    
    for bad in html.xpath( XpathToBeRemoved ):
        bad.getparent().remove(bad)
    
    ### Creat directed graph
    G = nx.DiGraph()
    dates, anchs = set(), set()
    dXpath = "//a[@href]|//*[normalize-space(text())]" # +"|//*[starts-with(@class,'date')]"
    elems = html.xpath( dXpath )
    elems = sorted(elems, key = lambda el: el.sourceline)
    for el in elems:
        if el.tag=='a':
            el.pdate = ''
            anchs.add(el)
        else:
            # print( GetText(el), '\n' )
            el.pdate = FindPublishDate( GetText(el).replace('\n', ' ') )
            if el.pdate != '':
                dates.add(el)
            else:
                continue
        
        G.add_node(el)
        el_ = el
        for an in el.iterancestors():
            if an.tag not in ['html', 'body']:
                G.add_edge(an, el_)
                el_ = an
    dates = sorted(dates, key=lambda d: d.sourceline)
    
    # for a in anchs: print('\n',tostring(a))
    #### Explore graph to find news items; any <a> in vicinity of a <date>    
    items = {}
    for d in dates: # d = list(dates)[1] # for d in dates: print( d.sourceline, tostring(d), d.text_content() )
        if d in G:
            shortPaths = nx.single_source_shortest_path(G.to_undirected(), d, cutoff=5)
        else:
            continue
        
        paths = [path for target, path in shortPaths.items() if target in anchs]
        if len(paths):
            paths = sorted(paths, key=lambda x: [len(x), x[-1].sourceline] )        
            a = paths[0][-1] # tostring(a)

            href = a.get('href')
            item = items.get(href, {})
            item['pdate'] = item.get('pdate', set()).union({d.pdate,})
            item['elems'] = item.get('elems', set()).union({a,})

            # Delete d and a
            lca = nx.lowest_common_ancestor(G, d, a, default=None) #tostring(lca)
            # lcaDates = {d.pdate for d in dates}
            
            otherDates = [el for el in nx.descendants(G, lca) if el in dates and el!=d]
            if len( otherDates ) < 2:      
                headings = lca.xpath(".//h1|.//h2|.//h3|.//h4|.//h5|.//h6") 
                if len( headings ):
                    titles = []
                    for h in headings:
                        txt = GetText( h )
                        if IsValidTitle(txt):
                            titles.append( txt )
                else:
                    titles = [txt for txt in GetText( lca ).split("\n") if IsValidTitle(txt)]
                G.remove_nodes_from( nx.descendants(G,lca) )
                G.remove_node( lca )
            else:
                titles = [txt for txt in GetText( a ).split("\n") if IsValidTitle(txt)]
                elemRem = set( nx.shortest_path(G, lca, a) )
                elemRem = elemRem.union( nx.shortest_path(G, lca, d) )
                G.remove_nodes_from( elemRem )
                
            item['titles'] = item.get('titles', []) + titles
            items[ href ] = item
        else:
            G.remove_node( d )
        
    #### Create news List
    p['newsLst'] = []
    for nurl, nitem in items.items(): #break
        if not IsValidUrl( nurl ) or nurl==url:
            continue
        n = News() 
        n.url = nurl
        n.title = nitem['titles'][0] if len(nitem['titles']) else '' 
        n.pdate = sorted( nitem['pdate'] )[0]
        p['newsLst'].append(n)
        # print(n.title)
        
    if len(p['newsLst']):
        p['newsLst'] = sorted(p['newsLst'], key=lambda x: x['pdate'])[::-1] 

    return p   
    
#%%
def DownloadNews(p, br):
    newsUrls, newsTitles = NewsUrlsTexts( p )
    if '' in newsUrls: newsUrls.remove('')
    if '' in newsTitles: newsTitles.remove('')

    for n in p['newsLst']: # n = p['newsLst'][0]
        # print(n.url)
        if n.url in newsUrls or n.title in newsTitles:
            n.exists = True
            continue
        
        try:
            n.urlPdf = n.url
            n = DoPdf(n)
            
            # n.chunk is the first KB of file downloaded in DoPdf
            if b'<html' in n.chunk.lower(): 
                n.urlHtml = n.url
                n = DoHtml(n, br)
                
        except Exception:
            log.exception( f"{p['coId']} _,_ {p['npUrl']} ==> {n.url}" )
#            log.exception( p['url'] )
            # raise 
                   
        p['timeStop'] = datetime.now()
        if (p['timeStop'] - p['timeStart']).seconds > p['timeOut']:
            break
        
    return p

#%%
def RemoveDuplicates( iLst ):
    oLst = []
    for s in iLst:
        if s not in oLst:
            oLst.append( s )
            
    return oLst

#%%
def DoNLP( p ):
   
    nlpFlag = False
    for n in p['newsLst']: # n = p['newsLst'][1]
        if n.exists: 
            continue
        elif not nlpFlag:
            from QBNLP import GetNLP
            nlp = GetNLP(commoditiesIdNm = p['commoditiesIdNm'],
                         commoditiesIdCode = p['commoditiesIdCode'])
            nlpFlag = True
            
        text = f" {n.title}\n{n.textHtml}\n{n.textPdf} "
        doc = nlp( text )
        
        #####
        n.commIds = RemoveDuplicates( doc._.commodities )
        
    return p 

#%%
def GetSummary( text ):
    summary = summarize(text, ratio=0.4, split=True)
    while len(summary):
        if any([w in summary[0] for w in ['Tel', 'Fax', 'Ph.', 'No.',
                'Website', 'Toll Free', 'Suite', 'Street', 'Avenue',
                'St.', 'Road', 'T:', 'F:', 'Number',] ]):
            summary.pop(0)
        else:
            break
        
    summary = '\n'.join( summary[:10] )
                        
    return summary

#%%   
def InsertIntoDB( p ):
    stuff = set()
    for n in p['newsLst']:
        if n.exists or n.url in stuff:
            continue
        stuff.add(n.url)
        
        if len( n.textHtml ) > len( n.textPdf ):
            text = n.textHtml
        else:
            text = n.textPdf
        
        try:
            n.summary = GetSummary( text )
        except:
            log.exception( f"{p['coId']} _,_ {p['npUrl']} ==> {n.url}" )
#            log.exception( n.url )

        if len(n.summary) and not IsValidTitle( n.title ):
            n.title = n.summary.split('\n')[0]
            
        text = text.replace('\'', '\'\'')
        n.summary = n.summary.replace('\'', '\'\'') 
        n.url = n.url.replace('\'', '\'\'')
        n.title = n.title.replace('\'', '\'\'')
        n.urlPdf = n.urlPdf.replace('\'', '\'\'')
        n.html = n.html.replace('\'', '\'\'')
        
        try:
            conn = pymysql.connect( *connString.split(':'), 3306)           
            q = f"""insert into news (nDateIns, nDatePub, coId, npId, 
                                    nTitle, nUrl, nText, nSummary, nHtml, nPdfUrl)
            values( CURDATE(), CAST('{n.pdate}' AS DATE), {p['coId']}, {p['npId']}
                   , N'{n.title}', N'{n.url}', N'{text}', N'{n.summary}'
                   , N'{n.html}', N'{n.urlPdf}' );
                    """
            with conn.cursor() as c:
                c = conn.cursor()
                c.execute( q )
                n.nId = conn.insert_id()
                
                for commId in n.commIds:
                    q = f"""insert into newsCommodities 
                        (nId, comId) values ( {n.nId}, {commId} )
                    """
                    c.execute( q )
            
            conn.commit()
            conn.close()
        except:
            log.exception( f"{p['coId']} _,_ {p['npUrl']} ==> {n.url}" )
            try:
                conn.close()
            except:
                pass
        
    return True

#%%

def DoPaper( p ):# p = papers[4]; print(p['npUrl'])
    try:
        p['timeOut'] = 15*60
        p['timeStart'] = datetime.now()
        p['newsLst'] = []
        options = Options()
        options.headless = True
        options.set_preference("security.sandbox.content.level", 5)
        options.set_preference("browser.privatebrowsing.autostart", True)
        options.set_preference("dom.disable_beforeunload", True)
        br = Firefox( options=options )

        br.set_page_load_timeout( 45 )
        br.maximize_window()
    except:
        log.exception( f"{p['coId']} _,_ {p['npUrl']}" )
        p['timeStop'] = p['timeStart']
        return p
    
    try:
        p = FindNews( p, br )
        p = DownloadNews(p, br)
    except:
        log.exception( f"{p['coId']} _,_ {p['npUrl']}" )
        
    br.quit()
    
    try:
        p = DoNLP( p )
        InsertIntoDB( p )
    except:
        log.exception( f"{p['coId']} _,_ {p['npUrl']}" )
    
    p['newsInserted'] = len([n for n in p['newsLst'] if not n.exists])
    p['newsTotal'] = len(p['newsLst'])
    p['timeStop'] = datetime.now()
        
    return p
#%%
def MultiProcess( papers ): #paper = p
    
    import psutil      
    i, p = 0, psutil.Process()
    while i<3:
        i += 1
        p = psutil.Process( p.ppid() )
        if p.name() in ['cmd.exe',]:
            with open('ScraperKiller.bat', 'w') as f:
                f.write(f"taskkill /pid {p.pid} /t /f")
                f.write(f"\r\n")
            break
        
    from multiprocessing import Pool
    from pandas import DataFrame as DF
    from tqdm import tqdm
    
    results = []
    with Pool( processes=15 ) as pool:
        for paper in tqdm( pool.imap_unordered(DoPaper, papers), 
                        total=len(papers), desc="Doing Papers" ):
            
            ##### Statistics
            paper['newsInserted'] = len([n for n in paper['newsLst'] if not n.exists])
            paper['newsTotal'] = len(paper['newsLst'])
            for key in ['sitesIdNm', 'commoditiesIdNm', 'commoditiesIdCode', 
                        'companiesIdNm', 'countriesIdNm', 'iospWords', 'newsLst']:
                paper.pop(key, None)
        
            results.append( paper )
            DF(results).to_csv("NewsFinderResults.csv", index=False, encoding='utf-8-sig')
                
    print( "All done!!!" )
#%%
def Analysis():
    from pandas import read_csv
    
    # df = read_csv('NewsFinderResults - Copy.csv', encoding = "UTF-8-sig")
    
    coIds=set(); 
    with open( 'r', ) as f:       
        for l in f: 
            try:
                coIds.add( int( l.split("_,_")[0] ) )
            except:
                pass
    set(papers['coId']) - coIds
    return
#%%
def GetPapers():
    # from tqdm import tqdm
#    conn = pymysql.connect( *connString.split(':') )
#    papers = read_sql("select * from newsPapers where npIsValid = 1", conn)
#    papers = papers.drop_duplicates(['npUrl']).sample(frac=1).to_dict('records')
#    conn.close()
    
    papers = DF( SQLExec( "select * from newsPapers where npIsValid = 1" ) )\
        .drop_duplicates(['npUrl']).sample(frac=1).to_dict('records')
    ############## nlp 
    commoditiesIdNm, commoditiesIdCode = GenerateCommodities()
    
    for p in papers:
        p['commoditiesIdNm'] = commoditiesIdNm
        p['commoditiesIdCode'] = commoditiesIdCode
        
    return papers        

#%%
def GenerateCommodities():      
    comms = []
    for r in SQLExec( f"select comId, comCode, comName from commodities"  ):
        # Non valid codes
        if r['comCode'] in ['St', 'Al', 'PGM', 'U', 'PE', 'CL', 'Bx', 'Bu', 'ATH',
                        'SUB', 'CE', 'IO', 'SA', 'SUA', 'V', 'COND', 'Ref Prod',
                        'SD', 'PL', 'Eco', 'FRT', 'EP', 'CPS', 'IP', 'OG', 'SH',
                        'AD', 'C2', 'CR', 'UC', 'ELEC', 'S', 'Ala', 'Co', 'Cr',]:
            r['comCode'] = r['comName']
            
        comms.append( (r['comId'], r['comCode'], r['comName'].lower())  )
    
    commoditiesIDNm = {}
    commoditiesIDCode = {}
    for commid, commcode, commname in comms:
        commoditiesIDCode[commid] = commcode
    
        commL = commoditiesIDNm.get(commid, set())
        commL.add( commname )
        commoditiesIDNm[commid] = commL
    
    return commoditiesIDNm, commoditiesIDCode
    
#%%
if __name__=="__main__":
    papers = GetPapers() 
#    MultiProcess( papers )
    
    