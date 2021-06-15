from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os
from sqlalchemy import text
from sqlalchemy.sql import text
from datetime import datetime
# import dateutil.parser
import math
# import time

#Init app
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
# #Database
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(basedir, 'db.sqlite')
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
ma = Marshmallow(app)


class Fundamental(db.Model):
	id = db.Column(db.Integer, primary_key=True)
	gvkey = db.Column(db.String(200))
	tic = db.Column(db.String(200))
	conm = db.Column(db.String(200))
	datadate = db.Column(db.String(200))
	rdq = db.Column(db.String(200))
	fyearq = db.Column(db.String(200))
	fqtr = db.Column(db.String(200))
	curcdq = db.Column(db.String(200))
	seqq = db.Column(db.String(200))
	ltq = db.Column(db.String(200))
	atq = db.Column(db.String(200))
	cheq = db.Column(db.String(200))
	niq = db.Column(db.String(200))
	piq = db.Column(db.String(200))
	ibq = db.Column(db.String(200))
	revtq = db.Column(db.String(200))
	txtq = db.Column(db.String(200))
	dlttq = db.Column(db.String(200))
	actq = db.Column(db.String(200))
	lctq = db.Column(db.String(200))
	cshoq = db.Column(db.String(200))


	def __init__(self, gvkey, tic, conm, datadate, rdq, fyearq, fqtr, curcdq, seqq, ltq, atq, cheq, niq, piq, ibq, revtq, txtq, dlttq, actq, lctq, cshoq):
		self.gvkey        = gvkey
		self.tic          = tic
		self.conm       = conm
		self.datadate         = datadate
		self.rdq         = rdq
		self.fyearq         = fyearq
		self.fqtr         = fqtr
		self.curcdq         = curcdq
		self.seqq         = seqq
		self.ltq         = ltq
		self.atq         = atq
		self.cheq         = cheq
		self.niq         = niq
		self.piq         = piq
		self.ibq         = ibq
		self.revtq         = revtq
		self.txtq         = txtq
		self.dlttq         = dlttq
		self.actq         = actq
		self.lctq         = lctq
		self.cshoq         = cshoq


# Product Schema
class FundamentalSchema(ma.Schema):
    class Meta:
       fields = ('id', 'gvkey', 'tic', 'conm', 'datadate', 'rdq', 'fyearq', 'fqtr', 'curcdq', 'seqq', 'ltq', 'atq', 'cheq', 'niq', 'piq', 'ibq', 'revtq', 'txtq', 'dlttq', 'actq', 'lctq', 'cshoq')

fundamental_schema = FundamentalSchema(many=True)

class Price(db.Model):
	id = db.Column(db.Integer, primary_key=True)
	gvkey = db.Column(db.String(200))
	iid = db.Column(db.String(200))
	datadate = db.Column(db.String(200))
	curcdd = db.Column(db.String(200))
	cshoc = db.Column(db.String(200))
	cshtrd = db.Column(db.String(200))
	prccd = db.Column(db.String(200))
	prchd = db.Column(db.String(200))
	prcld = db.Column(db.String(200))
	trfd = db.Column(db.String(200))

	def __init__(self, gvkey, iid, datadate, curcdd, cshoc, cshtrd, prccd, prchd, prcld, trfd):
		self.gvkey        = gvkey
		self.iid          = iid
		self.datadate       = datadate
		self.curcdd         = curcdd
		self.cshoc         = cshoc
		self.cshtrd         = cshtrd
		self.prccd         = prccd
		self.prchd         = prchd
		self.prcld         = prcld
		self.trfd         = trfd


# Product Schema
class PriceSchema(ma.Schema):
    class Meta:
       fields = ('id', 'gvkey', 'iid', 'datadate', 'curcdd', 'cshoc', 'cshtrd', 'prccd', 'prchd', 'prcld', 'trfd')

#price_schema = PriceSchema(strict=True)
price_schema = PriceSchema(many=True)

@app.route('/', methods=['GET'])
def det():
	sql = db.session.query(Fundamental).from_statement(
	   text("""SELECT * FROM fundamental""")	
		).all()
	result = fundamental_schema.dump(sql)
	# user = db.session.query(Price).from_statement(
	#    text("""SELECT * FROM price""")	
	# 	).all()
	# result = price_schema.dump(user)
	return jsonify({'msg': result})

@app.route('/all_companies/', methods=['GET'])
def get_companies():
    sql = db.session.query(Fundamental).from_statement(
	   text("SELECT * FROM fundamental GROUP BY gvkey")	
		).all()
    result = fundamental_schema.dump(sql)
    return jsonify({'companies_list': result})	


@app.route('/share_prices', methods=['POST'])
def get_prices():
	data = request.json
	tic = data.get('tic', '')
	from_date = data.get('from_date', '')
	to_date = data.get('to_date', '')
	
	#time_tuple = date.timetuple()
	#timestamp = time.mktime(time_tuple)
	sql = db.session.query(Fundamental).from_statement(
	   text("SELECT * FROM fundamental WHERE tic=:val LIMIT 1")	
		).params(val=tic).all()
	result = fundamental_schema.dump(sql)
	if len(result) > 0:
		gvkey = result[0]["gvkey"]
		try:
			start_date = math.floor((datetime.strptime(from_date, "%m/%d/%Y")).timestamp())
			end_date = math.floor((datetime.strptime(to_date, "%m/%d/%Y")).timestamp())
			sql = db.session.query(Price).from_statement(
				text("SELECT * FROM price WHERE gvkey =:val AND datadate BETWEEN  :val1 AND :val2")
				).params(val=gvkey, val1=start_date, val2= end_date).all()
			result = price_schema.dump(sql)
			return jsonify({'prices_list': result})
		except:
			return jsonify({'result': False})
			date = datetime.datetime.strptime(from_date, "%m/%d/%Y")
		
	else:
		return jsonify({'result': False})     

def myMapFunc(n):
	print(n)
	return n

@app.route('/accounting_data', methods=['POST'])
def get_account():
	data = request.json
	gvkey = data.get('gvkey', '')
	a = gvkey.replace(' ','')
	gvkey_arr = a.split(",")
	result_arr = []
	for x in gvkey_arr:
		sql = db.session.query(Fundamental).from_statement(
	   text("SELECT * FROM fundamental WHERE gvkey IN(:val)")	
		).params(val=x).all()
		result = fundamental_schema.dump(sql)
		for x in result:
			result_arr.append(x)
	
	
	return jsonify({'companies_list': result_arr})



#Rum Server
if __name__ == '__main__':
   app.run(debug=True)