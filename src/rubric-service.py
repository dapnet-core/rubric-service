#!/usr/bin/env python3
import requests
import json
import os
import pprint
import arrow

# TODO: Change for production in docker container
COUCHDB_USER = os.getenv('COUCHDB_USER', 'admin')
COUCHDB_PASSWORD = os.getenv('COUCHDB_PASSWORD', 'supersecret')
COUCHDB_HOST = 'dapnetdc2.db0sda.ampr.org'
COUCHDB_PORT = '5984'
COUCHDB_DBNAME = 'rubrics'

CALLS_URL = 'dapnetdc2.db0sda.ampr.org/calls/lowlevel'

PERSITENTFILE = '../localdata/state.seq'

CouchDBauth=(COUCHDB_USER, COUCHDB_PASSWORD)

RICRUBRICCONTENT=4520
RICRUBRICLABEL=4512
NONSKYPERRUBRICOFFSET=1000
pp = pprint.PrettyPrinter(indent=4)


def writeCurrentSequenceToFile(currentSeq):
  try:
    filewrite = open(PERSITENTFILE, 'w')
  except IOError:
    print("IOError: for file '%s'" % PERSITENTFILE)
    raise
  else:
    with filewrite:
      filewrite.write(currentSeq)
      filewrite.close()


def getLastSequenceFromFile():
  try:
    fileread = open(PERSITENTFILE, 'r')
  except IOError:
    print("IOError: File '%s' not present, creating it now..." % PERSITENTFILE)
    currentSequence = getCurrentSequence()
    writeCurrentSequenceToFile(currentSequence)
    return currentSequence
  else:
    with fileread:
      currentSequence = fileread.readlines()
      fileread.close()
      return currentSequence

def getCurrentSequence():
  r = requests.get('http://' + COUCHDB_HOST + ':' + COUCHDB_PORT + '/' + COUCHDB_DBNAME,
                   auth=CouchDBauth,
                   params={'include_docs' : 'false'})
  r.raise_for_status()
  answer = json.loads(r.text)
  return answer['update_seq']

def getLastChanges(seq):
  r = requests.get('http://' + COUCHDB_HOST + ':' + COUCHDB_PORT + '/' + COUCHDB_DBNAME + '/_changes',
               auth=CouchDBauth,
               params={'since': seq, 'include_docs': 'true'})
  r.raise_for_status
  answer = json.loads(r.text)
  return answer

def sendCall(data, ric, function, expires_on, priority, distribution):
  payload = {
    'data': data,
    'ric': ric,
    'function': function,
    'expires_on': expires_on,
    'priority': priority,
    'distribution': distribution,
    'type': 'alphanumeric'
  }
  pp.pprint (payload)
  #r = requests.post('http://' + CALLS_URL, data=payload)
  #r.raise_for_status


def sendCallfromRubricLabel(rubric):
  # rubric as complete object
  data = '1'
  data += chr(rubric['number'] + 0x1f)
  data += chr(10 + 0x20)

  for c in rubric['label']:
    data += chr(ord(c) + 1)

  priority = rubric['default_priority']
  distribution = {
    'transmitter_groups': rubric['transmitter_groups'],
    'transmitters': rubric['transmitters']
  }

  expires_on = arrow.utcnow().shift(seconds=rubric['default_expiration'])

  sendCall(data, RICRUBRICLABEL, 3, expires_on.isoformat() + 'Z', priority, distribution)

def sendCallfromRubricContent(rubric, index):
  # rubric as complete JSON object, index 0-9 for message slot
  print ('Index: ' + str(index) + ', Rubric: ' + rubric['_id'])
  print ('Message-Data: ' + rubric['content'][index]['data'])



  priority = rubric['default_priority']
  # Overwrite with priority if given
  if 'priority' in rubric['content'][index]:
    priority = rubric['content'][index]['priority']

  distribution = {
    'transmitter_groups': rubric['transmitter_groups'],
    'transmitters': rubric['transmitters']
  }

  expires_on = arrow.utcnow().shift(seconds=rubric['default_expiration'])

  # Send Call on RIC NONSKYPERRUBRICOFFSET + rubric number
  sendCall(rubric['content'][index]['data'], NONSKYPERRUBRICOFFSET + rubric['number'], rubric['function'],
           expires_on.isoformat() + 'Z', priority, distribution)

  #if function == 3, send skyper format
  if rubric['function']==3:
    # Encode message
    data = '1'
    data += chr(rubric['number'] + 0x1f)
    data += chr(index + 0x20)
    for c in rubric['content'][index]['data']:
      data += chr(ord(c) + 1)

    sendCall(data, RICRUBRICCONTENT, 3, expires_on.isoformat()  + 'Z', priority, distribution)


def sendAllContentOfRubric(rubric):
  print ('Send all Content of rubric ' + rubric['_id'])
  if 'content' in rubric:
    for index, message in enumerate(rubric['content']):
      if 'data' in message:
        if 'expires_on' in message:
          now = arrow.utcnow()
          expiration = arrow.get(message['expires_on'])
          if now < expiration:
            sendCallfromRubricContent(rubric, index)
        else:
          sendCallfromRubricContent(rubric, index)

def parseChanges(changes):
  results = changes['results']
  for result in results:
    if 'deleted' in result and result['deleted']:
      # rubric was deleted
      print ("Rubric " + result['id'] + ' was deleted')
    else:
      # rubric was changed
      if 'doc' in result:
        doc = result['doc']
        print (doc)
        # Send label
        sendCallfromRubricLabel(doc)
        # Send all content
        sendAllContentOfRubric(doc)

lastSeq = getLastSequenceFromFile()
lastChanges = getLastChanges(lastSeq)

# Send Name of changed rubrics in case they have been added in between
parseChanges(lastChanges)



pp.pprint(lastChanges)

# seq = 'now'
# while 1:
#   payload = {'include_docs': 'true', 'feed': 'longpoll', 'since': seq, 'heartbeat': '30000'}
#   r = requests.get('http://dapnetdc2.db0sda.ampr.org:5984/rubrics/_changes', auth=('admin', 'supersecret'), params=payload)
#   print("status: " + str(r.status_code))
#   print("content: " + r.text)
#   change_json=json.loads(r.text)
#   print (change_json)
#   seq=change_json['last_seq']
