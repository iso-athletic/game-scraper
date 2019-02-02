import os
from bs4 import BeautifulSoup
import requests
import time
import random
import unicodedata
import pandas as pd
import json
import sys
from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import re
import psycopg2


class GameScraper(object):

    def __init__(self, team_names, years):
        self.headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 1-_9_5) AppleWebKit 537.36 (KHTLM, like Gecko) Chrome",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"}
        self.workingDirectory = "./"
        self.allTeams = self.loadSchoolCodes()
        self.team_names = team_names
        self.years = years
        self.team_maps = {'Northwestern Wildcats': 1}
        # self.csv_collection = list()

    def loadSchoolCodes(self):
        print('Loading school data')
        schoolURL = "https://www.sports-reference.com/cbb/schools/"
        html = requests.get(schoolURL).content
        schoolNames = {}
        soup = BeautifulSoup(html, "html.parser")
        schools = soup.select('td[data-stat*="school_name"]')
        for school in schools:
            schoolCode = school.find('a').get('href').split("/")[-2]
            schoolNames[schoolCode] = schoolName = school.find('a').text

        with open('teamInfo.json', 'w') as outfile:
            json.dump(schoolNames, outfile)

        with open(self.workingDirectory+'/teamInfo.json') as infile:
            return json.load(infile)

    # find correct table in web page
    def getGameData(self, targetTeam, outputPath, gameDate, soup):
        print('Getting game data', targetTeam, gameDate)

        allTables = soup.select('table[id*="box-score"]')
        for index, teamStats in enumerate(allTables):
            if teamStats.caption.text.split(" ")[0] in str(targetTeam):
                targetTeamStats = allTables[index]
            else:
                opponent = teamStats.caption.text.replace(" Table", "")
                opponent = opponent.replace(opponent.split(" ")[-1], "")

        playerStatsDictionary = {}
        dataStatsDictionary = {}
        for index, individualStats in enumerate(targetTeamStats.find_all('tr')):
            if index == 1:
                for metricInfo in individualStats.find_all('th'):
                    try:
                        description = metricInfo['data-tip']
                        dataStatsDictionary[metricInfo['data-stat']
                                            ] = description
                    except Exception as e:
                        pass
            try:
                playerName = individualStats.find('th').text
                allIndividualStats = individualStats.find_all('td')
                if len(allIndividualStats) > 1:
                    playerStatsDictionary[playerName+''+gameDate] = {}
                    playerStatsDictionary[playerName+'' +
                                          gameDate]['player'] = playerName
                    playerStatsDictionary[playerName +
                                          ''+gameDate]['player_id'] = 0
                    playerStatsDictionary[playerName+'' +
                                          gameDate]['organization_id'] = self.team_maps[targetTeam]
                    # playerStatsDictionary[playerName+'' +
                    #                       gameDate]['opponent'] = opponent
                    playerStatsDictionary[playerName+'' +
                                          gameDate]['game_date'] = gameDate
                    for playerStats in allIndividualStats:
                        dataStatName = playerStats.attrs['data-stat']
                        dataStatValue = playerStats.text
                        playerStatsDictionary[playerName+'' +
                                              gameDate][dataStatName] = dataStatValue

            except Exception as e:
                pass
        return playerStatsDictionary

    def scrape(self):
        for year in self.years:
            for team_ in self.team_names:

                # setting things up
                allGameStatsDict = {}
                os.system("rm -r "+self.workingDirectory+"/"+team_)
                os.system("mkdir "+self.workingDirectory+"/"+team_)
                gamelogURL = "https://www.sports-reference.com/cbb/schools/" + \
                    team_+"/"+year+"-gamelogs.html"
                html = requests.get(gamelogURL).content
                soup = BeautifulSoup(html, "html.parser")
                # soup.select('td[data-stat*="date_game"]')
                gameDates = soup.find_all(
                    'td', attrs={"data-stat": "date_game"})
                for game in gameDates:
                    time.sleep(random.randint(1, 2))
                    gamelink = game.find('a').get('href')
                    gameDate = game.text
                    URL = 'https://www.sports-reference.com/'+gamelink
                    html = requests.get(URL).content
                    soup = BeautifulSoup(html, "html.parser")

                    allGameStatsDict.update(self.getGameData(
                        self.allTeams[team_], self.workingDirectory+team_, gameDate, soup))
                
                allGameStatsDict = { k:v for k,v in allGameStatsDict.items() if 'School Totals' not in k }

                df = pd.DataFrame(columns=list(allGameStatsDict[list(allGameStatsDict)[0]]), index=[
                    i for i in range(0, len(allGameStatsDict.keys()))])

                for index, player in enumerate(allGameStatsDict):
                    # if 'School Totals' in player:
                    #     continue
                    df.loc[index] = pd.Series(allGameStatsDict[player])
                df = df.sort_values(by=['game_date'])
                df.to_csv(self.workingDirectory+'/'+team_+'/'+team_.replace(",", "").replace(".", " ")+"_"+str(year)+".csv",
                          index=False)

    def insertIntoDb(self):
        print('Cleaning database')

        conn = psycopg2.connect("host="+os.eviron['pghost'] + " dbname="+os.environ['pgdb'] + " user="+os.environ['pguser'] + " password="+os.environ['pgpassword'] + " port="+os.environ['pgport'])
        cur = conn.cursor()

        cur.execute("""
                    DELETE from "Stats" 
                    WHERE player_id = '0'
                    """)
        conn.commit()

        SQL_STATEMENT = """
        COPY "Stats"(player_name,player_id,organization_id,date_scrimmage,mp,fg,fga,"fg%","2p","2pa","2p%","3p","3pa","3p%",ft,fta,"ft%",oreb,dreb,reb,ast,stl,blk,tov,pf,pts) FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
        """
        
        print('Copying into database')

        for team in self.team_names:
            for year in self.years:
                my_file = open(self.workingDirectory+'/' + team + '/' + team + '_' + year + '.csv')
                cur.copy_expert(sql=SQL_STATEMENT, file=my_file)
                conn.commit()

        cur.close()


if __name__ == "__main__":
    scraper = GameScraper(os.environ['teams'].split(',')), os.environ['years'].split(',')) # replace with variables
    scraper.scrape()
    scraper.insertIntoDb()
    print('Done!')



# #box score

# res = urlopen(req)
# rawpage = res.read()
# page = rawpage.replace("<!-->", "")
# soup = BeautifulSoup(page, "html.parser")

# #scorebox meta info
# scoreboxMeta = soup.find('div',class_="scorebox_meta").find_all('div')
# date = scoreboxMeta[0].text
# stadium = scoreboxMeta[1].text

# #score box
# soup.find('div',id="div_line-score")
# page = rawpage.replace("<!-->", "")

# soup.find_all('h2')
