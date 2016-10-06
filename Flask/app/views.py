# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template, request
from app import app

# importing Cassandra modules
from cassandra.cluster import Cluster


# Setting up connections to cassandra
cluster = Cluster(['ec2-52-45-70-95.compute-1.amazonaws.com'])

# Select the keyspace
session = cluster.connect('playground')



import random

with open("/home/ubuntu/app/firstname.csv",'r') as fn:
    fname_ls = fn.readlines()

with open("/home/ubuntu/app/surname.csv",'r') as fs:
    sname_ls = fs.readlines()

f_list = []
s_list = []

for f in range(1,101):
    fn = fname_ls[f].strip().split(',')[1].strip("\"")
    f_list.append(fn)


for s in range(1,101):
    sn = fname_ls[s].strip().split(',')[1].strip("\"")
    s_list.append(sn)

name_list = []

for finx in range(100):
    for sinx in range(100):
        name = f_list[finx]+' '+s_list[sinx]
        name_list.append(name)

#random.shuffle(name_list)

id_list = []

for g in range(1,9):
    for i in range(1000):
        user_id = 'user_' + str(g) + '_' + str(i)
        id_list.append(user_id)


user_id = {}

for unum in range(4000):
    user_id[name_list[unum]] = id_list[unum]





@app.route('/name')
def index():
  user = { 'nickname': 'Miguel' } # fake user
  mylist = [1,2,3,4]
  return render_template("index.html", title = 'Home', user = user, mylist = mylist)

@app.route('/base')
def base():
  return render_template("base.html")

@app.route('/')
@app.route('/index')
def healmon():
 return render_template("healmon.html")

@app.route('/', methods = ['POST'])
def healmon_search():
    s_name = request.form["search_name"]
    if s_name in user_id.keys():
        jsonresponse = {"name":s_name,"check":"Name Exists"}
        return jsonify(result = jsonresponse)
    else:
        return jsonify(result={"name":s_name,"check":"Name Not Exists"})



@app.route('/search',methods = ['POST'])
def search_name():
    s_name = request.form["search_name"]
    if s_name in user_id.keys():
        jsonresponse = {"name":s_name,"check":"Name Exists"}
        return jsonify(result = jsonresponse)
    else:
        return jsonify(result={"name":s_name,"check":"Name Not Exists"})


@app.route('/hrini/<usr>')
def get_hrini(usr):
    stmt = "SELECT * FROM sstream WHERE uid=%s limit 400"
    name_id = user_id[usr]
    response = session.execute(stmt,parameters = [name_id])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"uid":x.uid,"hr":x.avg,"time":x.time} for x in response_list]
    return jsonify(records = jsonresponse)




@app.route('/hr/<usr>')
def get_hr(usr):
    stmt = "SELECT * FROM sstream WHERE uid=%s limit 1"
    name_id = user_id[usr]
    response = session.execute(stmt,parameters = [name_id])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"uid":x.uid,"hr":x.avg,"time":x.time} for x in response_list]
    return jsonify(record = jsonresponse)


