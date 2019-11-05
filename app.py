from flask import Flask, render_template, session, request, redirect, url_for, make_response
from nltk.tokenize import word_tokenize 
import csv

apps = Flask(__name__)
apps.secret_key = 'recommender'

uid=''
purData={'asin':[],'rvrID':[],'rvrName':[],'ratings':[],'rvwTim':[]}
prodData={'asin':[],'title':[],'cat':[],'subCat':[],'imgUrl':[],'related':[],'salRank':[],'avgRate':[]}
brs=[]
sq=[]

def verifyUser(uid,pw):
    global purData
    if(uid in purData['rvrID'] and pw=='12345'):
        return True
    return False

def readData():
    global purData,prodData
    for i in purData.values():
        del i[:]
    for i in prodData.values():
        del i[:]
    csv.register_dialect('myDialect',delimiter='\t',quoting=csv.QUOTE_ALL,skipinitialspace=True)
    with open("products.txt","r") as f:
        reader = csv.DictReader(f,dialect='myDialect')
        for row in reader:
            prodData['asin'].append(row['asin'])
            prodData['title'].append(row['title'])
            prodData['subCat'].append(row['subCat'])
            prodData['cat'].append(row['cat'])
            prodData['imgUrl'].append(row['imgUrl'])
            if row['related']=='null':
                prodData['related'].append([])
            else:
                prodData['related'].append(eval(row['related']))
            prodData['salRank'].append(int(row['salRank']))
    t=[[] for i in range(len(prodData['asin']))]
    rd={}
    with open("processedData.txt","r") as f:
        reader = csv.DictReader(f,dialect='myDialect')
        for row in reader:
            purData['asin'].append(row['asin'])
            purData['rvrID'].append(row['reviewerID'])
            '''
            if row['reviewerID'] in rd:
                rd[row['reviewerID']]+=1
            else:
                rd[row['reviewerID']]=1
            '''
            purData['rvrName'].append(row['reviewerName'])
            purData['ratings'].append(float(row['ratings']))
            purData['rvwTim'].append(int(row['reviewTime']))
            t[prodData['asin'].index(row['asin'])].append(float(row['ratings']))
    prodData['noRvw']=[len(i) for i in t]
    prodData['avgRate']=[round(sum(i)/len(i),1) for i in t]
    for i in rd.keys():
        if rd[i]>2:
            print(rd[i],i)
    usrgrp()

def usrgrp():
    casin=''
    for i in range(len(purData['rvrID'])):
        if(casin!=purData['asin'][i]):
            casin=purData['asin'][i]
            brs.append(set())
        brs[-1].add(purData['rvrID'][i])
    print(len(brs[244]))

def findReleted(asin):    
    ind=prodData['asin'].index(asin)
    nn=[]
    for i in range(len(prodData['asin'])):
        if ind==i: continue
        cc=brs[i].intersection(brs[ind])
        if len(cc)>3:#len(brs[i].union(brs[ind]))*k
            nn.append((prodData['asin'][i],len(cc)))
    nn.sort(reverse=True,key=lambda a:a[1])
    return [i[0] for i in nn]


def similarity(s1,s2):
    X_set = set(word_tokenize(s1))
    Y_set = set(word_tokenize(s2))
    l1 =[];l2 =[] 
    rvector = X_set.union(Y_set)  
    for w in rvector: 
        if w in X_set: l1.append(1) # create a vector 
        else: l1.append(0) 
        if w in Y_set: l2.append(1) 
        else: l2.append(0) 
    c = 0
    
    # cosine formula  
    for i in range(len(rvector)): 
        c+= l1[i]*l2[i] 
    cosine = c / float((sum(l1)*sum(l2))**0.5) 
    return cosine

@apps.route('/login', methods=['GET', 'POST'])
def login():
    global uid,sq
    if request.method == 'GET':
        return render_template('Login.html')
    uid= request.form['uid']
    print(uid)
    pw= request.form['pw']
    if(verifyUser(uid,pw)):
        sq=[]
        return redirect('home')
    return render_template('Login.html')

@apps.route('/home', methods=['GET'])
def home():
    uname=''
    if request.method == 'GET':
        prods=[];casin=''
        for i in range(len(purData['asin'])):
            if(casin!=purData['asin'][i]):
                casin=purData['asin'][i]
                ind=prodData['asin'].index(casin)
                for q in sq:
                    cosine = similarity(q,prodData['title'][ind])
                    if(cosine!=0):
                        prods.append((prodData['asin'][ind],prodData['title'][ind],prodData['subCat'][ind],prodData['imgUrl'][ind],prodData['avgRate'][ind],prodData['salRank'][ind]))
            if(purData['rvrID'][i]==uid):
                if uname=='':
                    uname=purData['rvrName'][i]
                t=prodData['asin'].index(purData['asin'][i])
                #r=findReleted(purData['asin'][i])
                r=prodData['related'][t]
                for j in r:
                    try:
                        ind=prodData['asin'].index(j)
                        dup=False
                        for k in prods:
                            if k[0]==j: dup=True
                        if(not dup): prods.append((prodData['asin'][ind],prodData['title'][ind],prodData['subCat'][ind],prodData['imgUrl'][ind],prodData['avgRate'][ind],prodData['salRank'][ind]))
                    except:
                        continue
        prods.sort(key=lambda a: a[5])
        return render_template('home.html',prods=prods,uname=uname if uname!='null' else 'User',p='h')

@apps.route('/orders', methods=['GET'])
def orders():
    uname=''
    if request.method == 'GET':
        prods=[]
        for i in range(len(purData['asin'])):
            if(purData['rvrID'][i]==uid):
                ind=prodData['asin'].index(purData['asin'][i])
                prods.append((prodData['asin'][ind],prodData['title'][ind],prodData['subCat'][ind],prodData['imgUrl'][ind],prodData['avgRate'][ind]))
        return render_template('home.html',prods=prods,uname=uname if uname!='null' else 'User',p='o')

@apps.route('/search', methods=['GET'])
def search():
    uname=''
    q=request.args.get('q')
    sq.append(q)
    prods=[]
    for ind in range(len(prodData['asin'])):
        cosine = similarity(q,prodData['title'][ind]) 
        if(cosine!=0):
            prods.append((prodData['asin'][ind],prodData['title'][ind],prodData['subCat'][ind],prodData['imgUrl'][ind],prodData['avgRate'][ind],cosine))
    prods.sort(reverse=True,key=lambda a: a[5])
    return render_template('home.html',prods=prods,uname=uname if uname!='null' else 'User',p='s')

if __name__ == "__main__":
	readData()
	apps.run(host='0.0.0.0',debug=True)