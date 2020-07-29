import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date
from datetime import datetime
import time
import pandas as pd
import ratelimit as rt

#using rate limiter
@rt.limits(calls = 1, period = 4)
def callSheet(name, creds):
	client = gspread.authorize(creds)
	data = client.open(name).sheet1
	return data

# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
#client = gspread.authorize(creds)

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
#sheet = client.open("BugHerd COPY").sheet1

name = "BugHerd COPY"
sheet = callSheet(name, creds) 

# Extract and print all of the values
list_of_hashes = sheet.get_all_records() 
#print(list_of_hashes)

# currentRow = sheet.row_count -- this doesnt work prints len of whole spreadsheet (1000)
# print(currentRow) 
print(len(list_of_hashes))

currentRow = len(list_of_hashes) + 2

sheet.update_cell(currentRow, 1, "TODAY")
"""
sheet.update_cell(currentRow, 1, "Color") # UPDATES TEAM COLOR 

#format color to be the specific color: 

#if blue team 
colorCell = "A" + str(currentRow)
print(colorCell)

#BLUE
#sheet.format(colorCell,  {"backgroundColor": {
#      "red": 0.9,
#      "green": 0.7,
#      "blue": 0.1
#    }})

#ORANGE

sheet.format(colorCell, {"backgroundColor":{
	"red": 0.2,
	"green": 0.5,
	"blue": 0.9
	}})

today = date.today()
d1 = today.strftime("%m/%d")
sheet.update_cell(currentRow, 2, d1) # UPDATES todays date

sheet.update_cell(currentRow, 3, "Project Name") # updates project (clients) name

sheet.update_cell(currentRow, 4, "example@gmail.com") #updates guest email address

#### do some math to figure out the number of hours 
date_format = "%d-%m-%Y %H:%M:%S"
now = datetime.now()

#time1  = datetime.strptime(now, date_format)
pinTime  = datetime.strptime('14-7-2020 09:56:00', date_format)

diff = now - pinTime

#print ('Days & Overall hours from the above two dates')
#print days
days = diff.days
#print (str(days) + ' day(s)')
if (days >= 2):
	#print('2 days') #to spreadsheet
	sheet.update_cell(currentRow, 5, '2 Days')
#print overall hours
else: 
	days_to_hours = days * 24
	diff_btw_two_times = (diff.seconds) / 3600
	overall_hours = days_to_hours + diff_btw_two_times
	#print (str(overall_hours) + ' hours');
	hr = round(overall_hours)
	sheet.update_cell(currentRow, 5, str(hr) + ' hours')

#dt_string = now.strftime("%H:%M") + " hours"
#print("date and time =", dt_string)	
#sheet.update_cell(currentRow, 5, dt_string)

sheet.update_cell(currentRow, 6, "#pins") #number of pins
"""
## part two ## 
# search through data to find rows that have have client updated as blank (not yes)

print(list_of_hashes[2])
print(list_of_hashes[2]['Client Notified'])
newData = pd.DataFrame(list_of_hashes)
#print(newData)

"""for index, x in newData.iterrows():
	if x['Client Notified'] == 'Yes':
		print('drop')
		newData.drop([index])"""

firstRun = newData[newData['Client Notified'] != 'Yes']
todayData = firstRun[firstRun['Date'] != '']

print(todayData)
#myList = [todayData[0]['Team'], todayData[0]['Date'], todayData[0]['Client'], todayData[0]['Client Email'], todayData[0]['Last Updated'], todayData[0]['# of Pins'], todayData[0]['What Pins Completed'], todayData[0]['To Do'], todayData[0]['Assigned']]
myList  = todayData.values.tolist()
#print(myList)
num = 0 
time.sleep(100)

for items in myList:
	currentRow += 1
	sheet.append_row(myList[num], value_input_option='USER_ENTERED')
	print(myList[num][0])
	colorCell = "A" + str(currentRow)
	#time.sleep(10)  
	if myList[num][0] == 'Blue':
		sheet.format(colorCell,  {"backgroundColor": {
			"red": 0.2,
			"green": 0.5,
			"blue": 0.9
	    }})
	else:
		sheet.format(colorCell,  {"backgroundColor": {
			"red": 0.9,
			"green": 0.7,
			"blue": 0.1
		}}) 
	#time.sleep(10)  
	num += 1


