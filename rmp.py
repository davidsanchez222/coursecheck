import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import json
from datetime import datetime
import logging

logging.basicConfig(filename="rmp_pull.log", format='%(asctime)s %(message)s', filemode='w')
logger = logging.getLogger("RMP Data Pull")
logger.setLevel(logging.DEBUG)


class RMPSchoolData:
    def __init__(self, short_id):
        self.short_id = short_id
        self.time_of_pull = datetime.now()
        self.long_id = self.get_long_id()
        self.professors_num = self.get_professors_num()
        self.school_name = None

    def make_dataframe(self):
        return self.school_wide_pull()

    def get_long_id(self):
        logger.info("Getting long id")
        res = requests.get(f"https://www.ratemyprofessors.com/school/{self.short_id}")
        if res.status_code != 200:
            logger.error(f"Error {res.status_code}: {res.text}")
            return
        soup = BeautifulSoup(res.text, "html.parser")
        script_tag = soup.find('script', string=re.compile('window.__RELAY_STORE__'))
        pattern = re.compile(r'window.__RELAY_STORE__ = ({.*?});', re.DOTALL)
        match = pattern.search(script_tag.string)
        if match:
            logger.info("long_id found")
            relay_store = json.loads(match.group(1))
            long_id = list(relay_store.keys())[1]
            return long_id

        else:
            logger.info("Pattern not found in the script.")

    def get_professors_num(self):
        session = HTMLSession()
        logger.info("Getting number of professors from rendered html...")
        res = session.get(f"https://www.ratemyprofessors.com/search/professors/{self.short_id}?q=*", timeout=1100)
        if res.status_code != 200:
            logger.info(f"Error {res.status_code}: {res.text}")
            return
        res.html.render()
        soup = BeautifulSoup(res.html.html, "html.parser")
        h1_tag = soup.find('h1', attrs={"data-testid": "pagination-header-main-results"})
        professors_num = int(h1_tag.text.split()[0])
        return professors_num

    def school_wide_pull(self):
        url = "https://www.ratemyprofessors.com/graphql"

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic dGVzdDp0ZXN0"
        }

        query = """
        query TeacherSearchPaginationQuery($count: Int!, $cursor: String, $query: TeacherSearchQuery!) {
          search: newSearch {
            ...TeacherSearchPagination_search_1jWD3d
          }
        }
        
        fragment TeacherSearchPagination_search_1jWD3d on newSearch {
          teachers(query: $query, first: $count, after: $cursor) {
            didFallback
            edges {
              cursor
              node {
                ...TeacherCard_teacher
                id
                __typename
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
            resultCount
            filters {
              field
              options {
                value
                id
              }
            }
          }
        }
        
        fragment TeacherCard_teacher on Teacher {
          id
          legacyId
          avgRating
          numRatings
          ...CardFeedback_teacher
          ...CardSchool_teacher
          ...CardName_teacher
          ...TeacherBookmark_teacher
        }
        
        fragment CardFeedback_teacher on Teacher {
          wouldTakeAgainPercent
          avgDifficulty
        }
        
        fragment CardSchool_teacher on Teacher {
          department
          school {
            name
            id
          }
        }
        
        fragment CardName_teacher on Teacher {
          firstName
          lastName
        }
        
        fragment TeacherBookmark_teacher on Teacher {
          id
          isSaved
        }
        """

        variables = {
            "count": self.professors_num + 100,  # plus 100 to be sure that we get all professors
            "cursor": "YXJyYXljb25uZWN0aW9uOjMx",
            "query": {
                "text": "",
                "schoolID": self.long_id,  # KSU long id: "U2Nob29sLTQ4MQ=="
                "fallback": True,
                "departmentID": None
            }
        }

        logger.info("Pulling data for all professors...")
        res = requests.post(url, json={'query': query, 'variables': variables}, headers=headers)

        if res.status_code == 200:
            logger.info("Professor data successfully pulled")
        else:
            print(f"Error {res.status_code}: {res.text}")

        res_list = res.json()["data"]["search"]["teachers"]["edges"]
        extracted_data = []
        for entry in res_list:
            professor = entry["node"]
            professor_info = {
                'firstName': professor['firstName'],
                'lastName': professor['lastName'],
                'fullName': f"{professor['firstName']} {professor['lastName']}",
                'avgRating': professor['avgRating'],
                'avgDifficulty': professor['avgDifficulty'],
                'wouldTakeAgainPercent': professor['wouldTakeAgainPercent'],
                'numRatings': professor['numRatings'],
                'profLegacyId': professor['legacyId'],
                "schoolLongId": professor["school"]["id"],
                "schoolName": professor["school"]["name"]
            }
            extracted_data.append(professor_info)
            self.school_name = professor_info["schoolName"]

        df = pd.DataFrame(extracted_data)

        return df


# EXAMPLE USAGE:
KSU_ID = 481
ksu = RMPSchoolData(KSU_ID)
ksu_data = ksu.make_dataframe()
ksu_data.to_csv("rawKSU_data.csv", index=False)
