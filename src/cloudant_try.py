from cloudant.client import Cloudant
client = Cloudant('admin', 'supersecret', url='http://dapnetdc2.db0sda.ampr.org:5984', connect=True)

rubric_database = client['rubrics']
my_document = rubric_database['rwth-afu']

# Display the document
print(my_document)
# Iterate over an infinite _db_updates feed
db_updates = client.db_updates(feed='continuous', since='181-g1AAAAIbeJyV0UsOgjAQBuAGfC7cu9MjUNBCV3ITbTslSLBdqGs9gVfQm-hN9CbYAonEhAQ202TS_2umkyOEJqkLaKa00iBjpVN9POWm7TDEF0VRZKnLhgfTGEc-xn4Q_l9uifOlqXxTC6gUhCCCEwA0PSuQyV5JaEvHNr2t006ZZoSQKFl3fX9nhUstjEoB42BFWVdBDUxFV3MY5G6VWz0F9bkneimPSnlaZV4qHEImKeulvCrl_fsT6gWJF8leyqdSGnsBPwBGaXMv2Rdx0qU-', descending=True)
for db_update in db_updates:
#    if some_condition:
#        db_updates.stop()
    print(db_update)
print('Ende')
