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

allRubrics = {}

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
      return currentSequence[0]

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
  #pp.pprint (payload)
  #r = requests.post('http://' + CALLS_URL, data=payload)
  #r.raise_for_status


def sendCallfromRubricLabel(rubric):
  print('Sending Label for Rubric ' + rubric['_id'])
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
  print ('Sending Rubric: ' + rubric['_id'] + ', Index: ' + str(index) + ', Data: ' + rubric['content'][index]['data'])


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

def processInitialChanges(changes):
  results = changes['results']
  for result in results:
    if 'deleted' in result and result['deleted']:
      # rubric was deleted
      print ("Rubric " + result['id'] + ' was deleted')
    else:
      # rubric was changed
      if 'doc' in result:
        doc = result['doc']
        #print (doc)
        SendCompleteRubricwithLabel(doc)

def loadAllRubrics():
  r = requests.get('http://' + COUCHDB_HOST + ':' + COUCHDB_PORT + '/' + COUCHDB_DBNAME + '/_all_docs',
                   auth=CouchDBauth,
                   params={'reduce': 'false', 'include_docs': 'true'})
  r.raise_for_status()
  answer = json.loads(r.text)
  for row in answer['rows']:
    if not row['doc']['_id'].startswith('_'):
      id = row['doc']['_id']
      allRubrics[id] = row['doc']

def SendCompleteRubricwithLabel(rubric):
  # Send label
  sendCallfromRubricLabel(rubric)
  # Send all content
  sendAllContentOfRubric(rubric)


def DetectandSendUpdateCallsfromChanges(changes):
  results = changes['results']
  for result in results:
    if 'deleted' in result and result['deleted']:
      # rubric was deleted
      print ("Rubric " + result['id'] + ' was deleted')
      # Delete from local copy
      allRubrics.pop(result['id]'])
    else:
      # rubric was changed
      if 'doc' in result:
        newdoc = result['doc']
        #print (newdoc)
        id = newdoc['_id']
        # Check if rubric is completely new
        if not (id in allRubrics):
          print('Rubric ' + id + ' is new, sending label and complete content')
          SendCompleteRubricwithLabel(newdoc)

        # detect changes in transmitter_groups
        elif not (set(newdoc['transmitter_groups']) == set(allRubrics[id]['transmitter_groups'])):
          print('Rubric ' + id + ' has changes in transmitter_groups, sending label and complete content')
          SendCompleteRubricwithLabel(newdoc)

        # detect changes in transmitters
        elif not (set(newdoc['transmitters']) == set(allRubrics[id]['transmitters'])):
          print('Rubric ' + id + ' has changes in transmitters, sending label and complete content')
          SendCompleteRubricwithLabel(newdoc)

        # Here the rubric is not new and the transmitter_groups and transmitters are unchanged
        # Test for changes in content
        contentNew = newdoc['content']
        contentOld = allRubrics[id]['content']

        index = 0
        while (index < len(contentNew)) and (index < len(contentOld)):
          if contentOld[index]['data'] == contentNew[index]['data']:
            index = index + 1
            continue
          sendCallfromRubricContent(newdoc, index)
          index = index + 1

        # If there is more new content then old content, send it, to
        if len(contentNew) > len(contentOld):
          while (index < len(contentNew)):
            sendCallfromRubricContent(newdoc, index)
            index = index + 1

        # Update local copy
        allRubrics[id] = newdoc

print('Getting last Sequence from File')
lastSeq = getLastSequenceFromFile()
print('Last Sequence is: ' + lastSeq)

lastChanges = getLastChanges(lastSeq)
print('lastChanges are:')
pp.pprint(lastChanges)

# Send Name of changed rubrics in case they have been added in between
print('Processing Initial Changes')
processInitialChanges(lastChanges)
#writeCurrentSequenceToFile(lastSeq)

longPollSequence = getCurrentSequence()
print('Current Sequence is: ' + longPollSequence)
print('Loading all rubrics')
loadAllRubrics()

while 1:
  print('Starting long poll...')
  payload = {'include_docs': 'true', 'feed': 'longpoll', 'since': longPollSequence, 'heartbeat': '30000'}
  r = requests.get('http://dapnetdc2.db0sda.ampr.org:5984/rubrics/_changes',
                   auth=CouchDBauth,
                   params=payload)
  r.raise_for_status
  changes=json.loads(r.text)
  print('Changes are:')
  pp.pprint (changes)
  # Save the last seq. for next round
  longPollSequence=changes['last_seq']
  DetectandSendUpdateCallsfromChanges(changes)
  print('New local copy:')
  pp.pprint(allRubrics)
  print('New Current Sequence is: ' + longPollSequence)
