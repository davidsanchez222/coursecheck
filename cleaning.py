import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import math
#%%

# cleaning rmp data of duplicates
rmp_data = pd.read_csv("rawKSU_data.csv")
rmp_data["hasRMPDuplicate"] = False
duplicates = rmp_data[rmp_data["fullName"].duplicated(keep=False)]
dup_idx = duplicates.groupby("fullName")["numRatings"].idxmax()
no_dup = duplicates.loc[dup_idx]
no_dup["hasRMPDuplicate"] = True
rmp_data = rmp_data.drop_duplicates("fullName", keep=False)
rmp_data = pd.concat([rmp_data, no_dup], axis=0)

# rounding up the wouldTakeAgainPercent column
rmp_data["wouldTakeAgainPercent"] = np.ceil(rmp_data["wouldTakeAgainPercent"])

# removing professors who don't have any reviews
rmp_data = rmp_data[~((rmp_data["wouldTakeAgainPercent"] == -1) & (rmp_data["numRatings"] == 0))]

rmp_data = rmp_data.sort_index()

# large data pull has some incorrect information saying 0s all around when there are actual ratings
p = rmp_data[rmp_data["numRatings"] < 2]
p = p.sort_values(by=["wouldTakeAgainPercent"], ascending=False)
p = p.reset_index(drop=True)


numRatings = []
takeAgain = []
avgRating = []
avgDifficulty = []

# check for inconsistencies
# use ip rerouting to make sure we dont get ip banned
for i in tqdm(range(rmp_data.shape[0])):
    fullName = rmp_data.iloc[i, 2]
    raw_numRatings = rmp_data.iloc[i, 6]
    raw_takeAgain = math.ceil(rmp_data.iloc[i, 5])
    raw_avgRating = rmp_data.iloc[i, 3]
    raw_avgDifficulty = rmp_data.iloc[i, 4]
    res = requests.get(f"https://www.ratemyprofessors.com/professor/{rmp_data.iloc[i, 7]}")
    soup = BeautifulSoup(res.text, "html.parser")

    div1 = soup.find("div", {"class": "RatingValue__NumRatings-qw8sqy-0 jMkisx"})
    a_tag = div1.find("a").text
    print(a_tag)
    print(rf"{a_tag}")
    a_tag = a_tag.replace('\xa0', ' ')
    match = re.search(r'(\d+) ratings', a_tag)

    try:
        actual_takeAgain = int(soup.find_all("div", {"class": "FeedbackItem__FeedbackNumber-uof32n-1 kkESWs"})[0].string[:-1])
    except ValueError:
        "Value is N/A"
        actual_takeAgain = -1

    actual_numRatings = int(match.group(1))
    actual_avgRating = float(soup.find_all("div", {"class": "RatingValue__Numerator-qw8sqy-2 liyUjw"})[0].string)
    actual_avgDifficulty = float(soup.find_all("div", {"class": "FeedbackItem__FeedbackNumber-uof32n-1 kkESWs"})[1].string)

    if actual_numRatings != raw_numRatings:
        numRatings.append((fullName, actual_numRatings, raw_numRatings))

    if actual_takeAgain != raw_takeAgain:
        takeAgain.append((fullName, actual_takeAgain, raw_takeAgain))

    if actual_avgRating != raw_avgRating:
        avgRating.append((fullName, actual_avgRating, raw_avgRating))

    if actual_avgDifficulty != raw_avgDifficulty:
        avgDifficulty.append((fullName, actual_avgDifficulty, raw_avgDifficulty))


# do the above logic into dataframe with 10 columns
# first column is name, next 8 is 2 for each data point (actual, raw), last column is numRatings


if rmp_data["numRatings"] < 1:
    pass



# cleaning course availability data
courses = pd.read_html("allcourses.html")[0]
courses = courses.iloc[:, :-4]
cols = ["Select", "CRN", "Subj", "Crse", "Sec", "Cmp", "Cred", "Title", "Days", "Time", "Cap", "Act", "Rem", "WL Cap",
        "WL Act", "WL Rem", "Instructor", "Date (MM/DD)", "Location"]
courses.columns = cols
courses = courses.drop(["WL Cap", "WL Act", "WL Rem"], axis=1)
courses = courses[(courses["Instructor"] != "TBA") & (~courses["Instructor"].str.contains(","))]
courses["Instructor"] = courses["Instructor"].apply(lambda x: ' '.join(x.split()[:-1]))
courses["SubjCrse"] = courses["Subj"] + courses["Crse"]


# take the one with the highest ratings
# column in the final table bool hasDuplicate
# if True then link them the search query for the professor not the page
# if False, meaning there is only one RMP entry, link them to the webpage

# kennesaw campus search
courses = pd.read_html("allcourses.html")[0]
courses = courses[courses.iloc[:, 0].isin(["add to worksheet", "C"])]
courses = courses.iloc[:, :-4]
cols = ["Select", "CRN", "Subj", "Crse", "Sec", "Cmp", "Cred", "Title", "Days", "Time", "Cap", "Act", "Rem", "WL Cap",
        "WL Act", "WL Rem", "Instructor", "Date (MM/DD)", "Location"]
courses.columns = cols
courses = courses.drop(["WL Cap", "WL Act", "WL Rem"], axis=1)
courses = courses[(courses["Instructor"] != "TBA") & (~courses["Instructor"].str.contains(","))]
courses["Instructor"] = courses["Instructor"].apply(lambda x: ' '.join(x.split()[:-1]))
courses["SubjCrse"] = courses["Subj"] + courses["Crse"]
courses = courses[courses["Cmp"] == "Kennesaw Campus"]
courses = courses[~(courses["Time"] == "TBA")]
courses['Start_Time'] = courses['Time'].str.split('-').str[0].str.strip()
courses['End_Time'] = courses['Time'].str.split('-').str[1].str.strip()
courses['Start_Time_24hr'] = pd.to_datetime(courses["Start_Time"]).dt.strftime('%H:%M:%S')
courses["Cap"] = courses["Cap"].astype(int)


h = courses[courses["Days"].isin(["MWF"])]
h = h[h["Cap"] > 100]
