from flask import Flask, jsonify, request
from bigquery.client import get_client
app = Flask(__name__)
project_id = '650935460348'
json_key = 'ratekeys.json'

client = get_client(json_key_file=json_key, readonly=True)


@app.route("/by_daywisebounce_search")
def hello():
	i  = request.args.get('id')
	t  = request.args.get('to')
	f  = request.args.get('from')

	#query = "SELECT   sum( totals.visits ) FROM TABLE_DATE_RANGE([witty-gap:93878168.ga_sessions_],TIMESTAMP('2014-09-10'), TIMESTAMP('2016-10-17')) group each by totals.visits,having totals.visits>0"
	query = "SELECT HOUR(SEC_TO_TIMESTAMP(visitstarttime)) AS sessionhour,count(totals.bounces)/count(totals.visits) AS bouncerate FROM TABLE_DATE_RANGE([witty-gap:93878168.ga_sessions_], TIMESTAMP('"+ str(f) +"'), TIMESTAMP('"+str(t)+"')) GROUP BY sessionhour LIMIT 1000;"

	print("query: " + query)	

	job_id, _results = client.query(query)
	# Check if the query has finished running.
	complete, row_count = client.check_job(job_id)

	if complete:
		list = client.get_query_rows(job_id)
		return jsonify(results=list)	

	# Retrieve the results.
	list = client.get_query_rows(job_id)
	return jsonify(results=list)
	#return results

if __name__ == "__main__":
    app.run('localhost',port=5008)