import pymongo

my_client = pymongo.MongoClient(
    'mongodb+srv://alfredfoo:binary11@cluster0-xhobt.mongodb.net/test?retryWrites=true'
)

try:
    print("MongoDB version is %s" %
            my_client.server_info()['version'])
except pymongo.errors.OperationFailure as error:
    print(error)
    quit(1)

mydatabase=my_client["windowstestdatabase"]

mycollection=mydatabase["windowstestcollection"]

mycollection.insert_one({
	"_id":1,
	"Name":"Alfred",
	"Age":24
	})

mycollection.insert_many([
	{
	"_id":2,
	"Name":"Joeyee",
	"Age":25
	},

	{
	"_id":3,
	"Name":"Shawhong",
	"Age":25
	}
	])
