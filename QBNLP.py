# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 15:30:00 2019

@author: dariy
"""


#%%
from spacy.tokens import Doc, Span
from spacy import load
from re import compile as reCompile
from re import IGNORECASE as reIGNORECASE

#%%
class QBComponent(object): # self=AMEComponent()
    name = 'QBComponent'
    
    def __init__(self, commoditiesIdNm={}, commoditiesIdCode={}):
        
#        self.countriesIdNm = countriesIdNm if len(countriesIdNm) else None
        self.commoditiesIdNm = commoditiesIdNm if len(commoditiesIdNm) else None
        self.commoditiesIdCode = commoditiesIdCode if len(commoditiesIdCode) else None

        ########## stop words
        self.commStopWords = {    
            0: reCompile("partner|independ|executive|director|trade|negotiator"\
                +"|role|auditor|declar|represent|manager|offer|engage|long|time"\
                +"|underwriter|party|integrated|business|sponsor|joint|role"\
                +"|co-lead|agents|assignment|arranger|plaintiff|lender|engineer"\
                +"|player|dealer|private|rich|corporate|communications|email"\
                +"|advisor|arranger|regulation|estimator|consultant|up", reIGNORECASE )
                }

        ########## Commodities
        if self.commoditiesIdNm is not None:
            comPat = ''    
            for commid, names in self.commoditiesIdNm.items():
                namespat = '\W|\W'.join( names )
                comPat = comPat + f"|(?P<com{commid}>\\W{namespat}\\W)" 
            comPat = comPat.strip('|')
            
            comCodePat = ''    
            for commid, commcode in self.commoditiesIdCode.items():
                comCodePat = comCodePat + f"|(?P<com{commid}>\\W{commcode}\\W)" 
            comCodePat = comCodePat.strip('|')   
    
            self.comCodePatCompiled = reCompile(comCodePat)
            self.comPatCompiled = reCompile(comPat, reIGNORECASE)
            Span.set_extension('commodities', getter=self.Commodities, force=True)
        Doc.set_extension('commodities', default=[], force=True)
        
        
    def Commodities(self, span):
        string = ' ' + span.text + ' ' 
        commodities = []
        for match in self.comPatCompiled.finditer( string ):
            comm = int( match.lastgroup[3:] )
            if comm in self.commStopWords\
            and self.commStopWords[comm].search( string ) is not None:
                continue
            commodities.append( comm )
            
        for match in self.comCodePatCompiled.finditer( string ):
            comm = int( match.lastgroup[3:] )
            commodities.append( comm )
            
        return commodities
    
    #########
    def GetSpans(self, doc):
        if not len(doc):
            return []
        #for tok in doc: print(tok, tok.dep_, tok.pos_, tok.like_num, tok.lemma_, tok.shape_)

        chunks = list(doc.noun_chunks) + list(doc.ents) 
        for tok in doc: 
            if tok.like_num or tok.shape_.lower() in ['ddddxd', 'ddddxd', 'xxddxd']: 
#           tok.pos_ in ['PROPN']:
                chunks.append( Span(doc, tok.i, tok.i+1) )

        nces = sorted([ [nc.start, nc.end, nc] 
                        for nc in chunks if nc.text.strip() ])    
        if not len(nces):    
            return [Span(doc, 0, len(doc)),]
        
        spans = []
        start, end, span = nces[0]
        for start, end, nc in nces:        
            if span.end < start:
                spans.append(span)
                span = nc
            else:
                span = Span(doc, min(span.start, start), max(span.end, end) )
        if span not in spans:
            spans.append(span)   
        
        return spans

    #########
    def __call__(self, doc):
        doc.user_data['spans'] = self.GetSpans(doc)
        # countries, sites, commodities, companies = ([] for i in range(4))
        for span in doc.user_data['spans']:
#            span = doc.user_data['spans'][9]
                
            if self.commoditiesIdNm is not None\
            and self.commoditiesIdCode is not None: 
                doc._.commodities.extend(span._.commodities)

        
        return doc  
    
#%%
def GetNLP(commoditiesIdNm={}, commoditiesIdCode={},):
    
    nlp = load('en_core_web_sm') 
    
    QBComp = QBComponent(commoditiesIdNm, commoditiesIdCode)
    nlp.add_pipe( QBComp ) 

    return nlp


#%%
if __name__=="__main__":
    
    nlp = GetNLP(commoditiesIdNm, commoditiesIdCode, )
    
    QBComp = nlp.get_pipe('QBComponent')
    
    text = ' ' + """Hartley platinum project, Zimbabwe; Hot Briquetted Iron plant, 
              Yandi iron ore mine expansion and Beenup titanium minerals project, 
              Western Australia; Cannington silver, lead, zinc project and 
              Crinum coking coal mine, Queensland, Australia; 
              Mt Owen thermal coal development, New South Wales, Australia; 
              and Samarco pellet plant expansion, Brazil. 
              The Northwest Territories Diamonds project in Canada is subject to 
              a number of approvals. $41.669 million (1996 – $39.538 million). lead manager""" + ' '
              
    doc = nlp(text)

    print( doc.user_data, '\n')
#    print( [QBComp.countriesIdNm[ctrid] for ctrid in doc._.countries], '\n')
    print( [QBComp.commoditiesIdNm[commid] for commid in doc._.commodities] , '\n')
#    print( [QBComp.sitesIdNm[sid] for sid in doc._.sites] , '\n')
#    print( [QBComp.companiesIdNm[cid] for cid in doc._.companies] , '\n')
#    print( doc._.units, '\n', doc._.unitTypes , '\n')
#    
    from re import split as resplit, search as reSearch
    for doc in nlp.pipe( resplit('\n|\t|\r\n', text) ):
        for tok in doc:
            if tok._.hasUnit:
                print(tok.text, reSearch(' \d|[\d\.,]+', tok.text).group(),
                      tok._.ut, tok.sent.text)