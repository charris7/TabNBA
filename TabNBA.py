import requests
import json
import pandas as pd
import goldsberry
import csv
import datetime
import dataextract as tde

#NBA seasons to pull data for
seasons = ['2013-14','2014-15','2015-16','2016-17']

#This part grabs all of the unique player identifiers for the current NBA season
#Do not modify this section
players = goldsberry.PlayerList(Season='2016-17')
players = pd.DataFrame(players.players())
players = players['PERSON_ID'].tolist()
players = [str(i) for i in players]

#Create a base .csv file to append the shot chart data to
#Create a single row that includes only the column headers
with open('TabNBA.csv', 'w') as csvfile:
    fieldnames = ["SHOT_NUMBER","GRID_TYPE","GAME_ID","GAME_EVENT_ID","PLAYER_ID","PLAYER_NAME","TEAM_ID","TEAM_NAME","PERIOD","MINUTES_REMAINING","SECONDS_REMAINING","EVENT_TYPE","ACTION_TYPE","SHOT_TYPE","SHOT_ZONE_BASIC","SHOT_ZONE_AREA","SHOT_ZONE_RANGE","SHOT_DISTANCE","LOC_X","LOC_Y","SHOT_ATTEMPTED_FLAG","SHOT_MADE_FLAG","GAME_DATE","HTM","VTM","SEASON","DATE_OF_GAME"]#,"GAME_NUMBER"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

#Function that accepts a season and player, and appends all data received from the api to TabNBA.csv that was created in the step above
def shot_charts(season, player):
    shot_chart_url = 'http://stats.nba.com/stats/shotchartdetail?Period=0&VsConference=&LeagueID=00&LastNGames=0&TeamID=0&Position=&PlayerPosition=&Location=&Outcome=&ContextMeasure=FGA&DateFrom=&StartPeriod=&DateTo=&OpponentTeamID=0&ContextFilter=&RangeType=&Season='+season+'&AheadBehind=&PlayerID='+player+'&EndRange=&VsDivision=&PointDiff=&RookieYear=&GameSegment=&Month=0&ClutchTime=&StartRange=&EndPeriod=&SeasonType=Regular+Season&SeasonSegment=&GameID='
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}
    response = requests.get(shot_chart_url, headers=headers) 
    response.raise_for_status()
    data = json.loads(response.text)
    headers = data['resultSets'][0]['headers']
    shot_data = data['resultSets'][0]['rowSet']    
    df = pd.DataFrame(shot_data, columns=headers)
    df['SEASON'] = season
    df['DATE_OF_GAME'] = df['GAME_DATE'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
#    g = df.groupby(['PLAYER_ID','SEASON'])
#    g['GAME_ID'].rank(method='dense')
#    df['GAME_NUMBER']=g['GAME_ID'].rank(method='dense')
    with open('TabNBA.csv', 'a') as f:
        df.to_csv(f, header=False)

#Run the function above for all currently active NBA players, for all seasons in the seasons list    
for season in seasons:
    for player in players:
        shot_charts(season, player)

#Create TDE file
tdefile = tde.Extract('TabNBA.tde')

#Read data from the TabNBA.csv that was created in the steps above
csvReader = csv.reader(open('TabNBA.csv','rb'), delimiter=',', quotechar='"')

#Create TDE structure
if tdefile.hasTable('Extract'):
    table = tdefile.openTable('Extract')
    tableDef = table.getTableDefinition()
else: 
    tableDef = tde.TableDefinition()
    tableDef.addColumn('SHOT_NUMBER',         tde.Type.INTEGER)
    tableDef.addColumn('GRID_TYPE',           tde.Type.CHAR_STRING)
    tableDef.addColumn('GAME_ID',             tde.Type.INTEGER)
    tableDef.addColumn('GAME_EVENT_ID',       tde.Type.INTEGER)
    tableDef.addColumn('PLAYER_ID',           tde.Type.INTEGER)
    tableDef.addColumn('PLAYER_NAME',         tde.Type.CHAR_STRING)
    tableDef.addColumn('TEAM_ID',             tde.Type.INTEGER)
    tableDef.addColumn('TEAM_NAME',           tde.Type.CHAR_STRING)
    tableDef.addColumn('PERIOD',              tde.Type.INTEGER)
    tableDef.addColumn('MINUTES_REMAINING',   tde.Type.INTEGER)
    tableDef.addColumn('SECONDS_REMAINING',   tde.Type.INTEGER)
    tableDef.addColumn('EVENT_TYPE',          tde.Type.CHAR_STRING)
    tableDef.addColumn('ACTION_TYPE',         tde.Type.CHAR_STRING)
    tableDef.addColumn('SHOT_TYPE',           tde.Type.CHAR_STRING)
    tableDef.addColumn('SHOT_ZONE_BASIC',     tde.Type.CHAR_STRING)
    tableDef.addColumn('SHOT_ZONE_AREA',      tde.Type.CHAR_STRING)
    tableDef.addColumn('SHOT_ZONE_RANGE',     tde.Type.CHAR_STRING)
    tableDef.addColumn('SHOT_DISTANCE',       tde.Type.INTEGER)
    tableDef.addColumn('LOC_X',               tde.Type.INTEGER)
    tableDef.addColumn('LOC_Y',               tde.Type.INTEGER)
    tableDef.addColumn('SHOT_ATTEMPTED_FLAG', tde.Type.INTEGER)
    tableDef.addColumn('SHOT_MADE_FLAG',      tde.Type.INTEGER)
    tableDef.addColumn('GAME_DATE',           tde.Type.INTEGER)    
    tableDef.addColumn('HTM',                 tde.Type.CHAR_STRING)
    tableDef.addColumn('VTM',                 tde.Type.CHAR_STRING)
    tableDef.addColumn('SEASON',              tde.Type.CHAR_STRING)
    tableDef.addColumn('DATE_OF_GAME',        tde.Type.DATE)
    #tableDef.addColumn('GAME_NUMBER',         tde.Type.INTEGER)  
    table = tdefile.addTable('Extract',tableDef)
    
#Put data into the TDE
newrow = tde.Row(tableDef)
csvReader.next() 
for line in csvReader:    
    newrow.setInteger(0, int(line[0]))
    newrow.setCharString(1,line[1])
    newrow.setInteger(2, int(line[2]))
    newrow.setInteger(3, int(line[3]))
    newrow.setInteger(4, int(line[4]))
    newrow.setCharString(5,line[5])
    newrow.setInteger(6, int(line[6]))
    newrow.setCharString(7,line[7])
    newrow.setInteger(8, int(line[8]))
    newrow.setInteger(9, int(line[9]))
    newrow.setInteger(10,int(line[10]))
    newrow.setCharString(11,line[11])
    newrow.setCharString(12,line[12])
    newrow.setCharString(13,line[13])
    newrow.setCharString(14,line[14])
    newrow.setCharString(15,line[15])
    newrow.setCharString(16,line[16])
    newrow.setInteger(17, int(line[17]))
    newrow.setInteger(18, int(line[18]))
    newrow.setInteger(19, int(line[19]))
    newrow.setInteger(20, int(line[20]))
    newrow.setInteger(21, int(line[21]))
    newrow.setInteger(22, int(line[22]))
    newrow.setCharString(23,line[23])
    newrow.setCharString(24,line[24])
    newrow.setCharString(25,line[25])
    date = datetime.datetime.strptime(line[26], "%Y-%m-%d")
    newrow.setDate(26, date.year, date.month, date.day)
    #newrow.setInteger(27, int(line[27]))
    table.insert(newrow)

tdefile.close()