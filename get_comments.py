from nbsutils import imp
import urllib2
import json
import pickle
import config

def get_top_instagram_endpoints(limit):
	con = imp.Connection()
	query = """select e.*, idx.tot
	from meta_endpoints e
	inner join (
		select endpoint_id, max(value) tot 
		from idx_entity 
		where metric_id=253
		and count_type='t'
		group by endpoint_id
		order by max(value) desc
		limit {}
	) idx
	on idx.endpoint_id=e.id""".format(limit)
	df = con.fetchAll(query)
	con.close()
	user_ids = [int(json.loads(row['metadata'])['user_id']) for i,row in df.iterrows()]
	return dict(zip(user_ids,df['identifier'].tolist()))


def get_comments(user_ids,key):
	comments = {}
	for user_id in user_ids:
		print user_id
		try:
			user_url = "https://api.instagram.com/v1/users/{}/media/recent?count=50&access_token={}".format(user_id, key)
			user_data = json.loads(urllib2.urlopen(user_url).read())
		except urllib2.HTTPError,e:
			print "ERROR with url: "+user_url
			continue
		comments[user_id] = {}
		for post in user_data['data']:
			for comment in post['comments']['data']:
				comments[user_id][comment['id']] = comment['text']
	return comments

def save_comments(comments, endpoint_map, file_path):
	comments_and_map = {"comments": comments, "map":endpoint_map}
	pickle.dump( comments_and_map, open(file_path,"wb"))


print "getting comments from instagram"
comments = get_comments(endpoint_map.keys(), config.key)
print "storing comments"
save_comments(comments, endpoint_map, 'comments.pkl')

def main():
	print "getting top instagram accounts"
	endpoint_map = get_top_instagram_endpoints(100)

if __name__ == '__main__':
	main()
	
